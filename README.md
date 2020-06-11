# tada-io

Reading/writing tada DataFrames from other sources.

## Pandas DataFrames (via Apache Arrow)

`main.py`

(Using pyarrow version 0.16.0 and pandas version 1.0.1)
```
import pyarrow as pa
import pandas as pd


table = pa.Table.from_pandas(df)
batches = table.to_batches()
with open('foo.arrow', 'wb') as sink:
    writer = pa.RecordBatchFileWriter(sink, table.schema)

    for i in batches:
        writer.write_batch(i)

    writer.close()
```

`main.go`
```
import (
    "github.com/ptiger10/tada-io/arrow"
    "github.com/apache/arrow/go/arrow/ipc"
)

func main() {
    f, err := os.Open("foo.arrow")
    if err != nil {
        ...
    }
    defer f.Close()
		
    r, err := ipc.NewFileReader(f)
    if err != nil {
        ...
    }
    defer r.Close()

    df, err := arrow.FromReader(r)
    if err != nil {
        ...
    }
}
    
```

