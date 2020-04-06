package arrow

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/ptiger10/tada"
)

// -- FROM ARROW

// FromReader converts an arrio.Reader to a DataFrame
func FromReader(r arrio.Reader) (*tada.DataFrame, error) {
	var (
		ret    *tada.DataFrame
		schema *arrow.Schema
		i      int
	)
loop:
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break loop
			}
			return nil, fmt.Errorf("tada/arrow: could not read record %d: %w", i, err)
		}
		df, err := dataFrameFromRecordBatch(rec)
		if err != nil {
			return nil, fmt.Errorf("tada/arrow: record %d: converting Arrow to DataFrame: %w", i, err)
		}
		i++
		if ret == nil {
			ret = df
			schema = rec.Schema()
		} else {
			ret = ret.Append(df)
		}
	}
	// default label level used?
	if m := extractLabelNames(schema); len(m) == 0 {
		ret.InPlace().Relabel()
	}
	return ret, nil
}

func dataFrameFromRecordBatch(record array.Record) (*tada.DataFrame, error) {
	numContainers := int(record.NumCols())
	var err error
	containerNames := make([]string, numContainers)
	containerValues := make([]interface{}, numContainers)
	containerNulls := make([][]bool, numContainers)
	for k := 0; k < numContainers; k++ {
		containerNames[k] = record.ColumnName(k)
		containerValues[k], err = sliceFromArrowColumn(record.Column(k))
		if err != nil {
			return nil, fmt.Errorf("column %d: %v", k, err)
		}
		containerNulls[k] = nullsFromArrowColumn(record.Column(k))
	}
	labelNames, labelValues, labelNulls, colNames, colValues, colNulls := splitContainers(
		record.Schema(), containerNames, containerValues, containerNulls)

	df := tada.NewDataFrame(colValues, labelValues...).
		SetColNames(colNames)
	if df.Err() != nil {
		return nil, df.Err()
	}
	if labelNames != nil {
		df = df.SetLabelNames(labelNames)
	}
	for j := 0; j < len(labelNulls); j++ {
		err := df.SetNulls(j, labelNulls[j])
		if err != nil {
			return nil, err
		}
	}
	for k := 0; k < len(colNulls); k++ {
		err := df.SetNulls(k+df.NumLevels(), colNulls[k])
		if err != nil {
			return nil, err
		}
	}
	return df, nil
}

func nullsFromArrowColumn(column array.Interface) []bool {
	ret := make([]bool, column.Len())
	// no null values?
	if column.NullN() == 0 {
		for i := range ret {
			ret[i] = false
		}
		// at least one null value?
	} else {
		for i := range ret {
			ret[i] = column.IsNull(i)
		}
	}
	return ret
}

func splitContainers(
	schema *arrow.Schema,
	containerNames []string, containerValues []interface{}, containerNulls [][]bool,
) (labelNames []string, labelValues []interface{}, labelNulls [][]bool,
	colNames []string, colValues []interface{}, colNulls [][]bool) {

	labelNameSet := extractLabelNames(schema)
	labelPositions := extractPositions(labelNameSet, containerNames)

	// append label containers to the label objects, and non-label containers to the column objects
	for k := range containerNames {
		if _, ok := labelPositions[k]; ok {
			labelNames = append(labelNames, containerNames[k])
			labelValues = append(labelValues, containerValues[k])
			labelNulls = append(labelNulls, containerNulls[k])
		} else {
			colNames = append(colNames, containerNames[k])
			colValues = append(colValues, containerValues[k])
			colNulls = append(colNulls, containerNulls[k])
		}
	}
	return
}

func extractLabelNames(schema *arrow.Schema) map[string]bool {
	set := make(map[string]bool)
	if schema == nil {
		return set
	}

	// handle pandas index columns
	schemaKey := "pandas"
	key := schema.Metadata().FindKey(schemaKey)
	if key == -1 {
		return set
	}
	payload := schema.Metadata().Values()[key]

	// decode metadata payload at index_columns key
	payloadKey := "index_columns"
	m := make(map[string]interface{})
	json.NewDecoder(strings.NewReader(payload)).Decode(&m)
	indexNames, ok := m[payloadKey].([]interface{})
	if !ok {
		return set
	}

	for j := range indexNames {
		label, ok := indexNames[j].(string)
		// range index is stored in metadata as map[string]interface{}, and so will be ignored here.
		// use preserve_index if necessary: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pandas
		if !ok {
			return set
		}
		set[label] = true
	}
	return set
}

func extractPositions(set map[string]bool, containerNames []string) map[int]bool {
	ret := make(map[int]bool)

	// make set of int positions of index columns
	for k, name := range containerNames {
		if _, ok := set[name]; ok {
			ret[k] = true
		}
	}
	return ret
}
