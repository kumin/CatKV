package rowcodec

import (
	"time"

	"github.com/pingcap/tidb/types"
	"github.com/ugorji/go/codec"
)

type Decoder struct {
	requestColIDs []int64
	handleColID   int64
	requestTypes  []*types.FieldType
	origDefaults  [][]byte
	loc           *time.Location
}

func NewDecoder(requestColIDs []int64, handleColID int64, tps []*types.FieldType, origDefaults [][]byte,
	loc *time.Location) (*Decoder, error) {
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
		return encodeUint(nil, d.GetUint64), nil
	default:
		return defaultVal[1:], nil
	}
}
