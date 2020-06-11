package arrow_test

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/ptiger10/tada-io/arrow"
)

func ExampleFromReader() {
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

	df, err := arrow.FromReader(fr)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(df)

	// Output:
	// +-------------------++-----+-----+-------+---------------------------+
	// | __index_level_0__ || one | two | three |           four            |
	// |-------------------||-----|-----|-------|---------------------------|
	// |                 a ||  -1 | foo |  true | 2020-01-01T00:00:00-06:00 |
	// |                 b || n/a | n/a | false |                       n/a |
	// |                 c || 2.5 | baz |  true |                           |
	// +-------------------++-----+-----+-------+---------------------------+
}
