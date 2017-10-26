// Package restore is the restore subcommand for the influxd command,
// for restoring from a backup.
package restore

import (
	"archive/tar"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"bytes"
	"compress/gzip"
	"encoding/json"
	"github.com/influxdata/influxdb/cmd/influxd/backup"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/tcp"
	"log"
	"strings"
	"time"
)

// Command represents the program execution for "influxd restore".
type Command struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host string
	path string

	backupFilesPath     string
	metadir             string
	datadir             string
	destinationDatabase string
	sourceDatabase      string
	retention           string
	shard               string

	// TODO: when the new meta stuff is done this should not be exported or be gone
	MetaConfig *meta.Config

	shardIDMap map[uint64]uint64
}

// NewCommand returns a new instance of Command with default settings.
func NewCommand() *Command {
	return &Command{
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
		MetaConfig: meta.NewConfig(),
	}
}

// Run executes the program.
func (cmd *Command) Run(args ...string) error {
	// Set up logger.
	cmd.StdoutLogger = log.New(cmd.Stdout, "", log.LstdFlags)
	cmd.StderrLogger = log.New(cmd.Stderr, "", log.LstdFlags)

	if err := cmd.parseFlags(args); err != nil {
		return err
	}

	err := cmd.unpackMeta()
	if err != nil {
		cmd.StderrLogger.Printf("error: %v", err)
		return err
	}
	cmd.StdoutLogger.Println("Executing shard upload")

	err = cmd.uploadShardsLive()
	if err != nil {
		cmd.StderrLogger.Printf("error: %v", err)
		return err
	}
	//if cmd.metadir != "" {
	//	if err := cmd.unpackMeta(); err != nil {
	//		return err
	//	}
	//}
	//
	//if cmd.shard != "" {
	//	return cmd.unpackShard(cmd.shard)
	//} else if cmd.retention != "" {
	//	return cmd.unpackRetention()
	//} else if cmd.datadir != "" {
	//	return cmd.unpackDatabase()
	//}
	return nil
}

// parseFlags parses and validates the command line arguments.
func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&cmd.host, "host", "localhost:8088", "")
	fs.StringVar(&cmd.metadir, "metadir", "", "")
	fs.StringVar(&cmd.datadir, "datadir", "", "")
	fs.StringVar(&cmd.destinationDatabase, "database", "", "")
	fs.StringVar(&cmd.sourceDatabase, "origindb", "", "")
	fs.StringVar(&cmd.retention, "retention", "", "")
	fs.StringVar(&cmd.shard, "shard", "", "")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	}

	cmd.MetaConfig = meta.NewConfig()
	cmd.MetaConfig.Dir = cmd.metadir

	// Require output path.
	cmd.backupFilesPath = fs.Arg(0)
	if cmd.backupFilesPath == "" {
		return fmt.Errorf("path with backup files required")
	}

	// validate the arguments
	if cmd.destinationDatabase == "" {
		return fmt.Errorf("-database is a required parameter")
	}

	if cmd.sourceDatabase == "" {
		cmd.sourceDatabase = cmd.destinationDatabase
	}

	//if cmd.destinationDatabase != "" && cmd.datadir == "" {
	//	return fmt.Errorf("-datadir is required to restore")
	//}

	if cmd.shard != "" {
		if cmd.destinationDatabase == "" {
			return fmt.Errorf("-destinationDatabase is required to restore shard")
		}
		if cmd.retention == "" {
			return fmt.Errorf("-retention is required to restore shard")
		}
	} else if cmd.retention != "" && cmd.destinationDatabase == "" {
		return fmt.Errorf("-destinationDatabase is required to restore retention policy")
	}

	return nil
}

// unpackMeta reads the metadata from the backup directory and initializes a raft
// cluster and replaces the root metadata.
func (cmd *Command) unpackMeta() error {
	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", cmd.backupFilesPath)
	}

	latest := metaFiles[len(metaFiles)-1]

	fmt.Fprintf(cmd.Stdout, "Using metastore snapshot: %v\n", latest)
	// Read the metastore backup
	req := &snapshotter.Request{
		Type:     snapshotter.RequestMetaStoreUpdate,
		Database: cmd.destinationDatabase,
	}

	f, err := os.Open(latest)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	b := buf.Bytes()
	var i int

	// Make sure the file is actually a meta store backup file
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != snapshotter.BackupMagicHeader {
		return fmt.Errorf("invalid metadata file")
	}
	i += 8

	// Size of the meta store bytes
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]
	i += length

	var data meta.Data
	if err := data.UnmarshalBinary(metaBytes); err != nil {
		return fmt.Errorf("unmarshal: %s", err)
	} else {
		fmt.Println("successful unmarshal.  trying on the server side now.")
	}

	fmt.Println(metaBytes)

	resp, err := cmd.upload(req, bytes.NewReader(metaBytes), int64(length))
	if err != nil {
		return err
	}
	header := binary.BigEndian.Uint64(resp[:8])
	npairs := binary.BigEndian.Uint64(resp[8:16])
	pairs := resp[16:]

	if header != snapshotter.BackupMagicHeader {
		return fmt.Errorf("Response did not contain the proper header tag.")
	}

	if len(pairs)%16 != 0 || (len(pairs)/8)%2 != 0 {
		return fmt.Errorf("expected an even number of integer pairs in update meta repsonse")
	}

	cmd.shardIDMap = make(map[uint64]uint64)
	for i := 0; i < int(npairs); i++ {
		offset := i * 16
		k := binary.BigEndian.Uint64(pairs[offset : offset+8])
		v := binary.BigEndian.Uint64(pairs[offset+8 : offset+16])
		cmd.shardIDMap[k] = v
	}
	fmt.Printf("shard id map: %v", cmd.shardIDMap)
	return err
}

// unpackShard will look for all backup files in the path matching this shard ID
// and restore them to the data dir
func (cmd *Command) unpackShard(shardID string) error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase, cmd.retention, shardID)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shard already present: %s", restorePath)
	}

	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	// find the shard backup files
	pat := filepath.Join(cmd.backupFilesPath, fmt.Sprintf(backup.BackupFilePattern, cmd.destinationDatabase, cmd.retention, id))
	return cmd.unpackFiles(pat + ".*")
}

// unpackDatabase will look for all backup files in the path matching this destinationDatabase
// and restore them to the data dir
func (cmd *Command) unpackDatabase() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("destinationDatabase already present: %s", restorePath)
	}

	// find the destinationDatabase backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.destinationDatabase)
	return cmd.unpackFiles(pat + ".*")
}

// unpackRetention will look for all backup files in the path matching this retention
// and restore them to the data dir
func (cmd *Command) unpackRetention() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase, cmd.retention)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retention already present: %s", restorePath)
	}

	// find the retention backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.destinationDatabase)
	return cmd.unpackFiles(fmt.Sprintf("%s.%s.*", pat, cmd.retention))
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) unpackFiles(pat string) error {
	fmt.Printf("Restoring from backup %s\n", pat)

	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}

	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files for %s in %s", pat, cmd.backupFilesPath)
	}

	for _, fn := range backupFiles {
		if err := cmd.unpackGzip(fn); err != nil {
			return err
		}
	}

	return nil
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) uploadShardsLive() error {

	// gets DB, RP, shardID from a path string.
	//a := strings.Split(path, string(filepath.Separator))
	//if len(a) != 3 {
	//	return "", "", fmt.Errorf("expected destinationDatabase, retention policy, and shard id in path: %s", path)
	//}

	// find the destinationDatabase backup files
	pat := fmt.Sprintf("%s.*", filepath.Join(cmd.backupFilesPath, cmd.sourceDatabase))

	fmt.Printf("Restoring from backup %s\n", pat)

	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}

	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files for %s in %s", pat, cmd.backupFilesPath)
	}

	fmt.Println(backupFiles)
	for _, fn := range backupFiles {
		fmt.Println(fn)
		parts := strings.Split(fn, ".")

		if len(parts) != 4 {
			cmd.StderrLogger.Printf("Skipping mis-named backup file: %s", fn)
		}
		shardID, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return err
		}

		newShardID := cmd.shardIDMap[shardID]

		conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
		if err != nil {
			return err
		}

		conn.Write([]byte{byte(snapshotter.RequestShardUpdate)})

		// 0.  write the shard ID to pw
		shardBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(shardBytes, newShardID)
		conn.Write(shardBytes)
		// 1.  open TAR reader for file
		f, err := os.Open(fn)

		if err != nil {
			return err
		}
		tr := tar.NewReader(f)

		tw := tar.NewWriter(conn)

		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				tw.Close()
				f.Close()
				conn.Close()
				return err
			}

			names := strings.Split(hdr.Name, "/")
			hdr.Name = filepath.ToSlash(filepath.Join(cmd.destinationDatabase, names[1], strconv.FormatUint(newShardID, 10), names[3]))

			tw.WriteHeader(hdr)
			if _, err := io.Copy(tw, tr); err != nil {
				tw.Close()
				f.Close()
				conn.Close()
				return err
			}
		}
		tw.Close()
		f.Close()
		conn.Close()
	}

	return nil
}

// unpackGzip will restore a single tar archive to the data dir
func (cmd *Command) unpackGzip(gzFile string) error {
	f, err := os.Open(gzFile)
	if err != nil {
		return err
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()

	for {
		zr.Multistream(false)

		destinationFile, err := cmd.prepareDestinationFile(zr.Name)
		if err != nil {
			return err
		}
		ff, err := os.Create(destinationFile)
		if err != nil {
			return err
		}
		defer ff.Close()

		if _, err := io.Copy(ff, zr); err != nil {
			return err
		}

		err = zr.Reset(f)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}

}

// unpackTar will restore a single tar archive to the data dir
func (cmd *Command) unpackTar(tarFile string) error {
	f, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	tr := tar.NewReader(f)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if err := cmd.unpackTarFile(tr, hdr.Name); err != nil {
			return err
		}
	}
}

// unpackTarFile will copy the current file from the tar archive to the data dir
func (cmd *Command) unpackTarFile(tr *tar.Reader, fileName string) error {
	nativeFileName := filepath.FromSlash(fileName)
	fn := filepath.Join(cmd.datadir, nativeFileName)
	fmt.Printf("unpacking %s\n", fn)

	if err := os.MkdirAll(filepath.Dir(fn), 0777); err != nil {
		return fmt.Errorf("error making restore dir: %s", err.Error())
	}

	ff, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer ff.Close()

	if _, err := io.Copy(ff, tr); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) prepareDestinationFile(fileName string) (string, error) {
	nativeFileName := filepath.FromSlash(fileName)
	fn := filepath.Join(cmd.datadir, nativeFileName)
	fmt.Printf("unpacking %s\n", fn)

	if err := os.MkdirAll(filepath.Dir(fn), 0777); err != nil {
		return "", fmt.Errorf("error making restore dir: %s", err.Error())
	}

	return fn, nil

}

// unpackTarFile will copy the current file from the tar archive to the data dir
func (cmd *Command) unpackGzipFile(tr *tar.Reader, fileName string) error {
	nativeFileName := filepath.FromSlash(fileName)
	fn := filepath.Join(cmd.datadir, nativeFileName)
	fmt.Printf("unpacking %s\n", fn)

	if err := os.MkdirAll(filepath.Dir(fn), 0777); err != nil {
		return fmt.Errorf("error making restore dir: %s", err.Error())
	}

	ff, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer ff.Close()

	if _, err := io.Copy(ff, tr); err != nil {
		return err
	}

	return nil
}

// upload takes a request object, attaches a Base64 encoding to the request, and sends it to the snapshotter service.
func (cmd *Command) upload(req *snapshotter.Request, upStream io.Reader, nbytes int64) ([]byte, error) {

	req.UploadSize = nbytes
	var err error
	var bytesProcessed uint64
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		if err = func() error {
			// Connect to snapshotter service.

			conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

			conn.Write([]byte{byte(req.Type)})

			if req.Type != snapshotter.RequestShardUpdate { // Write the request
				if err := json.NewEncoder(conn).Encode(req); err != nil {
					return fmt.Errorf("encode snapshot request: %s", err)
				}
			}

			if n, err := io.Copy(conn, upStream); (err != nil && err != io.EOF) || n != req.UploadSize {
				return fmt.Errorf("error uploading file: err=%v, n=%d, uploadSize: %d", err, n, req.UploadSize)
			}

			//var response bytes.Buffer
			//// Read snapshot from the connection
			cmd.StdoutLogger.Printf("wrote %d bytes", req.UploadSize)

			b.Reset()
			if n, err := b.ReadFrom(conn); err != nil || n == 0 {
				return fmt.Errorf("copy backup to file: err=%v, n=%d", err, n)
			}
			bytesProcessed = binary.BigEndian.Uint64(b.Bytes()[8:])

			return nil
		}(); err == nil {
			break
		} else if err != nil {
			cmd.StderrLogger.Printf("Upload data failed %s.  Retrying (%d)...\n", err, i)
			time.Sleep(time.Second)
		}
	}

	return b.Bytes(), err
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	fmt.Fprintf(cmd.Stdout, `Uses backups from the PATH to restore the metastore, databases,
retention policies, or specific shards. The InfluxDB process must not be
running during a restore.

Usage: influxd restore [flags] PATH

    -metadir <path>
            Optional. If set the metastore will be recovered to the given path.
    -datadir <path>
            Optional. If set the restore process will recover the specified
            destinationDatabase, retention policy or shard to the given directory.
    -destinationDatabase <name>
            Optional. Required if no metadir given. Will restore the destinationDatabase
            TSM files.
    -retention <name>
            Optional. If given, destinationDatabase is required. Will restore the retention policy's
            TSM files.
    -shard <id>
            Optional. If given, destinationDatabase and retention are required. Will restore the shard's
            TSM files.

`)
}
