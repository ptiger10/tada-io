package arrow

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func sliceFromArrowColumn(column array.Interface) (interface{}, error) {
	numRows := column.Len()
	var ret interface{}
	switch column.(type) {

	case *array.Int8:
		arr := make([]int8, numRows)
		vals := column.(*array.Int8)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Int16:
		arr := make([]int16, numRows)
		vals := column.(*array.Int16)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Int32:
		arr := make([]int32, numRows)
		vals := column.(*array.Int32)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Int64:
		arr := make([]int64, numRows)
		vals := column.(*array.Int64)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Uint8:
		arr := make([]uint8, numRows)
		vals := column.(*array.Uint8)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Uint16:
		arr := make([]uint16, numRows)
		vals := column.(*array.Uint16)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Uint32:
		arr := make([]uint32, numRows)
		vals := column.(*array.Uint32)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Uint64:
		arr := make([]uint64, numRows)
		vals := column.(*array.Uint64)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Float32:
		arr := make([]float32, numRows)
		vals := column.(*array.Float32)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Float64:
		arr := make([]float64, numRows)
		vals := column.(*array.Float64)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Date32:
		arr := make([]int32, numRows)
		vals := column.(*array.Date32)
		for i := range arr {
			arr[i] = int32(vals.Value(i))

		}
		ret = arr

	case *array.Date64:
		arr := make([]int64, numRows)
		vals := column.(*array.Date64)
		for i := range arr {
			arr[i] = int64(vals.Value(i))

		}
		ret = arr

	case *array.Time32:
		arr := make([]int32, numRows)
		vals := column.(*array.Time32)
		for i := range arr {
			arr[i] = int32(vals.Value(i))

		}
		ret = arr

	case *array.Time64:
		arr := make([]int64, numRows)
		vals := column.(*array.Time64)
		for i := range arr {
			arr[i] = int64(vals.Value(i))

		}
		ret = arr

	case *array.Duration:
		arr := make([]int64, numRows)
		vals := column.(*array.Duration)
		for i := range arr {
			arr[i] = int64(vals.Value(i))

		}
		ret = arr

	case *array.Timestamp:
		arr := make([]time.Time, numRows)
		vals := column.(*array.Timestamp)
		loc := time.UTC
		t := vals.DataType().(*arrow.TimestampType)
		if tz := t.TimeZone; tz != "" {
			var err error
			loc, err = time.LoadLocation(tz)
			if err != nil {
				return nil, fmt.Errorf("reading location: %v", err)
			}
		}
		for i := range arr {
			arr[i] = time.Unix(0, int64(vals.Value(i))).In(loc)
		}
		ret = arr

	case *array.String:
		arr := make([]string, numRows)
		vals := column.(*array.String)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	case *array.Boolean:
		arr := make([]bool, numRows)
		vals := column.(*array.Boolean)
		for i := range arr {
			arr[i] = vals.Value(i)
		}
		ret = arr

	default:
		return nil, fmt.Errorf("unsupported type: %v", reflect.TypeOf(column))
	}
	return ret, nil
}

func sliceToArrowInterface(slice interface{}, notNulls []bool) (array.Interface, arrow.DataType) {
	pool := memory.NewGoAllocator()
	switch slice.(type) {

	case []int8:
		b := array.NewInt8Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]int8), notNulls)

		return b.NewInt8Array(), &arrow.Int8Type{}

	case []int16:
		b := array.NewInt16Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]int16), notNulls)

		return b.NewInt16Array(), &arrow.Int16Type{}

	case []int32:
		b := array.NewInt32Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]int32), notNulls)

		return b.NewInt32Array(), &arrow.Int32Type{}

	case []int64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]int64), notNulls)

		return b.NewInt64Array(), &arrow.Int64Type{}

	case []uint8:
		b := array.NewUint8Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]uint8), notNulls)

		return b.NewUint8Array(), &arrow.Uint8Type{}

	case []uint16:
		b := array.NewUint16Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]uint16), notNulls)

		return b.NewUint16Array(), &arrow.Uint16Type{}

	case []uint32:
		b := array.NewUint32Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]uint32), notNulls)

		return b.NewUint32Array(), &arrow.Uint32Type{}

	case []uint64:
		b := array.NewUint64Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]uint64), notNulls)

		return b.NewUint64Array(), &arrow.Uint64Type{}

	case []float32:
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]float32), notNulls)

		return b.NewFloat32Array(), &arrow.Float32Type{}

	case []float64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		b.AppendValues(slice.([]float64), notNulls)

		return b.NewFloat64Array(), &arrow.Float64Type{}

	case []time.Time:
		vals := slice.([]time.Time)
		b := array.NewTimestampBuilder(pool, &arrow.TimestampType{
			Unit:     arrow.Nanosecond,
			TimeZone: vals[0].Location().String(),
		})
		defer b.Release()
		ret := make([]arrow.Timestamp, len(vals))
		for i := range vals {
			ret[i] = arrow.Timestamp(vals[i].UnixNano())
		}
		b.AppendValues(ret, notNulls)

		return b.NewTimestampArray(), &arrow.TimestampType{}

	case []string:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		b.AppendValues(slice.([]string), notNulls)

		return b.NewStringArray(), &arrow.StringType{}

	case []bool:
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		b.AppendValues(slice.([]bool), notNulls)

		return b.NewBooleanArray(), &arrow.BooleanType{}

	case []int:
		vals := slice.([]int)
		ret := make([]int64, len(vals))
		for i := range vals {
			ret[i] = int64(vals[i])
		}
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendValues(ret, notNulls)

		return b.NewInt64Array(), &arrow.Int64Type{}

	case []uint:
		vals := slice.([]uint)
		ret := make([]uint64, len(vals))
		for i := range vals {
			ret[i] = uint64(vals[i])
		}
		b := array.NewUint64Builder(pool)
		defer b.Release()
		b.AppendValues(ret, notNulls)

		return b.NewUint64Array(), &arrow.Uint64Type{}

	default:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		v := reflect.ValueOf(slice)
		ret := make([]string, v.Len())
		for i := range ret {
			ret[i] = fmt.Sprint(v.Index(i).Interface())
		}
		b.AppendValues(ret, notNulls)

		return b.NewStringArray(), &arrow.StringType{}
	}
}
