package arrow

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/ptiger10/tada"
)

// -- TO ARROW

func invertNulls(nulls []bool) []bool {
	ret := make([]bool, len(nulls))
	for i := range nulls {
		ret[i] = !nulls[i]
	}
	return ret
}

// ToArrow converts a DataFrame to an arrow Record
func ToArrow(df *tada.DataFrame) (array.Record, error) {
	df.InPlace().DeduplicateNames()
	numContainers := df.NumLevels() + df.NumColumns()
	containerValues := make([]array.Interface, numContainers)
	fields := make([]arrow.Field, numContainers)
	labelNames := df.ListLabelNames()
	colNames := df.ListColNames()
	for j := 0; j < df.NumLevels(); j++ {
		s := df.SelectLabels(labelNames[j])
		fields[j].Name = labelNames[j]
		// fields[j].Metadata = arrow.MetadataFrom(map[string]string{"field_name": labelNames[j]})
		fields[j].Nullable = true
		containerValues[j], fields[j].Type = sliceToArrowInterface(
			s.GetValues(),
			invertNulls(s.GetNulls()))
		defer containerValues[j].Release()
	}
	for k := 0; k < df.NumColumns(); k++ {
		// offset by label levels
		index := df.NumLevels() + k
		s := df.Col(colNames[k])
		fields[index].Name = colNames[k]
		// fields[index].Metadata = arrow.MetadataFrom(map[string]string{"field_name": colNames[k]})
		fields[index].Nullable = true
		containerValues[index], fields[index].Type = sliceToArrowInterface(
			s.GetValues(),
			invertNulls(s.GetNulls()))
		defer containerValues[index].Release()
	}
	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecord(schema, containerValues, int64(df.Len()))
	defer rec.Release()
	return rec, nil
}
