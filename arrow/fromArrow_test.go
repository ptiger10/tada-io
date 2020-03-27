package arrow

import (
	"fmt"
	"log"
	"os"
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

func TestFromArrow(t *testing.T) {
	f, err := os.Open("tableFrom.arrow")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fr, err := ipc.NewFileReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()
	df, err := FromArrow(fr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(df)
}
