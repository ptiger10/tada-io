package arrow

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/ptiger10/tada"
)

// TO FIX: ToArrow currently saves valid Record, but cannot re-read Arrow file in either pyarrow or go.
// Probably related to how metadata is written?
//
// Error messages:
// pyarrow: pyarrow.lib.ArrowInvalid: Ran out of field metadata, likely malformed
// go: panic: runtime error: slice bounds out of range [88:80]

func TestToArrow(t *testing.T) {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 3, 5},
		[]string{"foo", "bar", ""},
	})
	rec, err := ToArrow(df)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create("tableTo.arrow")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w, err := ipc.NewFileWriter(f, ipc.WithSchema(rec.Schema()))
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	err = w.Write(rec)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rec)
}
