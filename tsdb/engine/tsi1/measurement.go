package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/influxdata/influxdb/pkg/rhh"
)

// MeasurementBlockVersion is the version of the measurement block.
const MeasurementBlockVersion = 1

// Measurement flag constants.
const (
	MeasurementTombstoneFlag = 0x01
)

// Measurement field size constants.
const (
	// Measurement trailer fields
	MeasurementBlockVersionSize = 2
	MeasurementBlockSize        = 8
	MeasurementHashOffsetSize   = 8
	MeasurementTrailerSize      = MeasurementBlockVersionSize + MeasurementBlockSize + MeasurementHashOffsetSize

	// Measurement key block fields.
	MeasurementNSize      = 4
	MeasurementOffsetSize = 8
)

// Measurement errors.
var (
	ErrUnsupportedMeasurementBlockVersion = errors.New("unsupported meaurement block version")
	ErrMeasurementBlockSizeMismatch       = errors.New("meaurement block size mismatch")
)

// MeasurementBlock represents a collection of all measurements in an index.
type MeasurementBlock struct {
	data     []byte
	hashData []byte

	version int // block version
}

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (blk *MeasurementBlock) Version() int { return blk.version }

// Elem returns an element for a measurement.
func (blk *MeasurementBlock) Elem(name []byte) (e MeasurementElem, ok bool) {
	n := binary.BigEndian.Uint32(blk.hashData[:MeasurementNSize])
	hash := hashKey(name)
	pos := int(hash) % int(n)

	// Track current distance
	var d int

	for {
		// Find offset of measurement.
		offset := binary.BigEndian.Uint64(blk.hashData[MeasurementNSize+(pos*MeasurementOffsetSize):])

		// Evaluate name if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e MeasurementElem
			e.UnmarshalBinary(blk.data[offset:])

			// Return if name match.
			if bytes.Equal(e.Name, name) {
				return e, true
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(e.Name), pos, int(n)) {
				return MeasurementElem{}, false
			}
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++
	}
}

// UnmarshalBinary unpacks data into the block. Block is not copied so data
// should be retained and unchanged after being passed into this function.
func (blk *MeasurementBlock) UnmarshalBinary(data []byte) error {
	// Parse version.
	if len(data) < MeasurementBlockVersion {
		return io.ErrShortBuffer
	}
	versionOffset := len(data) - MeasurementBlockVersionSize
	blk.version = int(binary.BigEndian.Uint16(data[versionOffset:]))

	// Ensure version matches.
	if blk.version != MeasurementBlockVersion {
		return ErrUnsupportedMeasurementBlockVersion
	}

	// Parse size & validate.
	szOffset := versionOffset - MeasurementBlockSize
	sz := binary.BigEndian.Uint64(data[szOffset:])
	if uint64(len(data)) != sz+MeasurementTrailerSize {
		return ErrMeasurementBlockSizeMismatch
	}

	// Parse hash index offset.
	hoffOffset := szOffset - MeasurementHashOffsetSize
	hoff := binary.BigEndian.Uint64(data[hoffOffset:])

	// Save data block & hash block.
	blk.data = data[:hoff]
	blk.hashData = data[hoff:hoffOffset]

	return nil
}

// MeasurementElem represents an internal measurement element.
type MeasurementElem struct {
	Flag   byte   // flag
	Name   []byte // measurement name
	Offset uint64 // tag set offset

	Series struct {
		N    uint32 // series count
		Data []byte // serialized series data
	}
}

// SeriesID returns series ID at an index.
func (e *MeasurementElem) SeriesID(i int) uint32 {
	return binary.BigEndian.Uint32(e.Series.Data[i*SeriesIDSize:])
}

// SeriesIDs returns a list of decoded series ids.
func (e *MeasurementElem) SeriesIDs() []uint32 {
	a := make([]uint32, e.Series.N)
	for i := 0; i < int(e.Series.N); i++ {
		a[i] = e.SeriesID(i)
	}
	return a
}

// UnmarshalBinary unmarshals data into e.
func (e *MeasurementElem) UnmarshalBinary(data []byte) error {
	// Parse flag data.
	e.Flag, data = data[0], data[1:]

	// Parse tagset offset.
	e.Offset, data = binary.BigEndian.Uint64(data), data[8:]

	// Parse name.
	sz, n := binary.Uvarint(data)
	e.Name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series data.
	v, n := binary.Uvarint(data)
	e.Series.N, data = uint32(v), data[n:]
	e.Series.Data = data[:e.Series.N*SeriesIDSize]

	return nil
}

// MeasurementBlockWriter writes a measurement block.
type MeasurementBlockWriter struct {
	mms map[string]measurement
}

// NewMeasurementBlockWriter returns a new MeasurementBlockWriter.
func NewMeasurementBlockWriter() *MeasurementBlockWriter {
	return &MeasurementBlockWriter{
		mms: make(map[string]measurement),
	}
}

// Add adds a measurement with series and offset.
func (mw *MeasurementBlockWriter) Add(name []byte, offset uint64, seriesIDs []uint32) {
	mm := mw.mms[string(name)]
	mm.offset = offset
	mm.seriesIDs = seriesIDs
	mw.mms[string(name)] = mm
}

// Delete marks a measurement as tombstoned.
func (mw *MeasurementBlockWriter) Delete(name []byte) {
	mm := mw.mms[string(name)]
	mm.deleted = true
	mw.mms[string(name)] = mm
}

// WriteTo encodes the measurements to w.
func (mw *MeasurementBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

	// Build key hash map
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(mw.mms),
		LoadFactor: 90,
	})
	for name := range mw.mms {
		mm := mw.mms[name]
		m.Put([]byte(name), &mm)
	}

	// Encode key list.
	offsets := make([]int64, m.Cap())
	for i := 0; i < m.Cap(); i++ {
		k, v := m.Elem(i)
		if v == nil {
			continue
		}
		mm := v.(*measurement)

		// Save current offset so we can use it in the hash index.
		offsets[i] = n

		// Write measurement
		if err := mw.writeMeasurementTo(w, k, mm, &n); err != nil {
			return n, err
		}
	}

	// Save starting offset of hash index.
	hoff := n

	// Encode hash map length.
	if err := writeUint32To(w, uint32(m.Cap()), &n); err != nil {
		return n, err
	}

	// Encode hash map offset entries.
	for i := range offsets {
		if err := writeUint64To(w, uint64(offsets[i]), &n); err != nil {
			return n, err
		}
	}

	// Write trailer.
	if err = mw.writeTrailerTo(w, hoff, &n); err != nil {
		return n, err
	}

	return n, nil
}

// writeMeasurementTo encodes a single measurement entry into w.
func (mw *MeasurementBlockWriter) writeMeasurementTo(w io.Writer, name []byte, mm *measurement, n *int64) error {
	// Write flag & tagset block offset.
	if err := writeUint8To(w, mm.flag(), n); err != nil {
		return err
	}
	if err := writeUint64To(w, mm.offset, n); err != nil {
		return err
	}

	// Write measurement name.
	if err := writeUvarintTo(w, uint64(len(name)), n); err != nil {
		return err
	}
	if err := writeTo(w, name, n); err != nil {
		return err
	}

	// Write series count & ids.
	if err := writeUvarintTo(w, uint64(len(mm.seriesIDs)), n); err != nil {
		return err
	}
	for _, seriesID := range mm.seriesIDs {
		if err := writeUint32To(w, seriesID, n); err != nil {
			return err
		}
	}

	return nil
}

// writeTrailerTo encodes the trailer containing sizes and offsets to w.
func (mw *MeasurementBlockWriter) writeTrailerTo(w io.Writer, hoff int64, n *int64) error {
	// Save current size of the write.
	sz := *n

	// Write hash index offset, total size, and v
	if err := writeUint64To(w, uint64(hoff), n); err != nil {
		return err
	}
	if err := writeUint64To(w, uint64(sz), n); err != nil {
		return err
	}
	if err := writeUint16To(w, MeasurementBlockVersion, n); err != nil {
		return err
	}
	return nil
}

type measurement struct {
	deleted   bool
	offset    uint64
	seriesIDs []uint32
}

func (mm measurement) flag() byte {
	var flag byte
	if mm.deleted {
		flag |= MeasurementTombstoneFlag
	}
	return flag
}