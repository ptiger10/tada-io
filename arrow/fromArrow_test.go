package arrow

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow/ipc"
)

/*
main.py

import pyarrow as pa
import numpy as np
import pandas as pd

df = pd.DataFrame(
    {
        'one': [-1,np.NaN, 2.5],
        'two': ['foo', None, 'baz'],
        'three': [True, False, True],
        'four': ['2020-01-01', None, None],
    },
    index=list('abc'),
    )

df['four'] = pd.to_datetime(df['four']).dt.tz_localize('US/Central')

table = pa.Table.from_pandas(df)
batches = table.to_batches(2)
with open('tableFrom.arrow', 'wb') as sink:
    writer = pa.RecordBatchFileWriter(sink, table.schema)

    for i in batches:
        writer.write_batch(i)

    writer.close()

*/

func TestFromReader(t *testing.T) {
	f, err := os.Open("tableFrom.arrow")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	fr, err := ipc.NewFileReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer fr.Close()

	df, err := FromReader(fr)
	if err != nil {
		t.Fatal(err)
	}

	got := new(strings.Builder)
	fmt.Fprintf(got, "%v", df)

	want := `+-------------------++-----+-----+-------+---------------------------+
| __index_level_0__ || one | two | three |           four            |
|-------------------||-----|-----|-------|---------------------------|
|                 a ||  -1 | foo |  true | 2020-01-01T00:00:00-06:00 |
|                 b || n/a | n/a | false |                       n/a |
|                 c || 2.5 | baz |  true |                           |
+-------------------++-----+-----+-------+---------------------------+
`

	if got, want := got.String(), want; got != want {
		t.Fatalf("invalid dataframe:\ngot:\n%s\nwant:\n%s\n",
			got, want,
		)
	}
}
