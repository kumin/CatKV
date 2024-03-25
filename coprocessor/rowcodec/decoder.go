package rowcodec

import (
	"math"
	"time"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

type Decoder struct {
	row
	requestColIDs []int64
	handleColID   int64
	requestTypes  []*types.FieldType
	origDefaults  [][]byte
	loc           *time.Location
}

func NewDecoder(
	requestColIDs []int64,
	handleColID int64,
	tps []*types.FieldType,
	origDefaults [][]byte,
	loc *time.Location,
) (*Decoder, error) {
	xOrigDefaultVals := make([][]byte, len(origDefaults))
	for i := 0; i < len(origDefaults); i++ {
		if len(origDefaults[i]) == 0 {
			continue
		}
		xDefaultVal, err := convertDefaultValue(origDefaults[i])
		if err != nil {
			return nil, err
		}
		xOrigDefaultVals[i] = xDefaultVal
	}

	return &Decoder{
		requestColIDs: requestColIDs,
		handleColID:   handleColID,
		requestTypes:  tps,
		origDefaults:  xOrigDefaultVals,
		loc:           loc,
	}, nil
}

func convertDefaultValue(defaultVal []byte) (colVal []byte, err error) {
	var d types.Datum
	_, d, err = codec.DecodeOne(defaultVal)
	if err != nil {
		return
	}
	switch d.Kind() {
	case types.KindNull:
		return nil, nil
	case types.KindInt64:
		return encodeInt(nil, d.GetInt64()), nil
	case types.KindUint64:
		return encodeUint(nil, d.GetUint64()), nil
	case types.KindString, types.KindBytes:
		return d.GetBytes(), nil
	case types.KindFloat32:
		return encodeUint(nil, uint64(math.Float32bits(d.GetFloat32()))), nil
	case types.KindFloat64:
		return encodeUint(nil, uint64(math.Float64bits(d.GetFloat64()))), nil
	default:
		return defaultVal[1:], nil
	}
}

func (decoder *Decoder) Decode(rowData []byte, handle int64, chk *chunk.Chunk) error {
	err := decoder.setRowData(rowData)
	if err != nil {
		return err
	}
	for colIdx, colID := range decoder.requestColIDs {
		if colID == decoder.handleColID {
			chk.AppendInt64(colIdx, handle)
			continue
		}
		i, j := 0, int(decoder.numNotNullCols)
		var found bool
		for i < j {
			h := int(uint(i+j) >> 1)
			var v int64
			if decoder.large {
				v = int64(decoder.colIDs32[h])
			}
			if v < colID {
				i = h + 1
			} else if v > colID {
				j = h
			} else {
				found = true
				colData := decoder.getData(h)
				err := decoder.decodeColData(colIdx, colData, chk)
				if err != nil {
					return err
				}
			}
		}
		if found {
			continue
		}
	}

	return nil
}

func (decoder *Decoder) decodeColData(colIdx int, colData []byte, chk *chunk.Chunk) error {
	return nil
}
