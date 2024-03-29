package rowcodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const CodecVer = 128

var invalidCodecVer = errors.New("invalid codec version")

const (
	NilFlag          byte = 0
	BytesFlag        byte = 1
	CompactBytesFlag byte = 2
	IntFlag          byte = 3
	UintFlag         byte = 4
	VarintFlag       byte = 8
	VaruintFlag      byte = 9
)

type row struct {
	large          bool
	numNotNullCols uint16
	numNullCols    uint16
	colIDs         []byte

	offsets []uint16
	data    []byte

	colIDs32  []uint32
	offsets32 []uint32
}

func (r row) String() string {
	var colValStrs []string
	for i := 0; i < int(r.numNotNullCols); i++ {
		var colID, offStart, offEnd int64
		if r.large {
			colID = int64(r.colIDs32[i])
			if i != 0 {
				offStart = int64(r.offsets32[i-1])
			}
			offEnd = int64(r.offsets32[i])
		} else {
			colID = int64(r.colIDs[i])
			if i != 0 {
				offStart = int64(r.offsets[i-1])
			}
			offEnd = int64(r.offsets[i])
		}
		colValData := r.data[offStart:offEnd]
		colValStr := fmt.Sprintf("(%d:%v)", colID, colValData)
		colValStrs = append(colValStrs, colValStr)
	}
	return strings.Join(colValStrs, ",")
}

func (r *row) getData(i int) []byte {
	var start, end uint32
	if r.large {
		if i > 0 {
			start = uint32(r.offsets32[i-1])
		}
		end = uint32(r.offsets32[i])
	} else {
		if i > 0 {
			start = uint32(r.offsets[i-1])
		}
		end = uint32(r.offsets[i])
	}
	return r.data[start:end]
}

func (r *row) setRowData(rowData []byte) error {
	if rowData[0] != CodecVer {
		return invalidCodecVer
	}
	r.large = rowData[1]&1 > 0
	r.numNotNullCols = binary.LittleEndian.Uint16(rowData[2:])
	r.numNullCols = binary.LittleEndian.Uint16(rowData[4:])
	cursor := 6
	if r.large {
		colIDsLen := int(r.numNotNullCols+r.numNullCols) * 4
		r.colIDs32 = bytesToU32Slice(rowData[cursor : cursor+colIDsLen])
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 4
		r.offsets32 = bytesToU32Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	} else {
		colIDsLen := int(r.numNotNullCols + r.numNullCols)
		r.colIDs = rowData[cursor : cursor+colIDsLen]
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 2
		r.offsets = bytes2U16Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	}
	return nil
}

func encodeInt(buf []byte, iVal int64) []byte {
	var tmp [8]byte
	if int64(int8(iVal)) == iVal {
		buf = append(buf, byte(iVal))
	} else if int64(int16(iVal)) == iVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(iVal))
		buf = append(buf, tmp[:4]...)
	}

	return buf
}

func decodeInt(val []byte) int64 {
	switch len(val) {
	case 1:
		return int64(int8(val[0]))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(val)))
	default:
		return int64(binary.LittleEndian.Uint64(val))
	}
}

func encodeUint(buf []byte, iVal uint64) []byte {
	var tmp [8]byte
	if uint64(uint8(iVal)) == iVal {
		buf = append(buf, byte(iVal))
	} else if uint64(uint16(iVal)) == iVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(iVal))
		buf = append(buf, tmp[:2]...)
	}

	return buf
}

func bytes2U16Slice(b []byte) []uint16 {
	return nil
}

func bytesToU32Slice(b []byte) []uint32 {
	return nil
}
