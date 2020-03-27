package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

// Generic is a generic type
type Generic struct {
	Name        string
	Type        string
	ConvertFrom bool
	ExcludeFrom bool
}

func main() {
	var (
		tmpl,
		genericSrc string
	)
	flag.StringVar(&tmpl, "template", "", "the source of the template to parse")
	flag.StringVar(&genericSrc, "data", "", "the source of the generics")

	flag.Parse()

	genericInput, err := ioutil.ReadFile(genericSrc)
	if err != nil {
		log.Fatalf("reading file (%s): %s", genericSrc, err)
	}

	t, err := template.New(tmpl).ParseFiles(tmpl)
	if err != nil {
		log.Fatalf("processing template (%s): %s", tmpl, err)
	}

	var generics []Generic
	err = json.Unmarshal(genericInput, &generics)
	if err != nil {
		log.Fatalf("unmarshaling input: %s", err)
	}
	b := new(bytes.Buffer)
	err = t.Execute(b, generics)
	if err != nil {
		log.Fatalf("executing template: %s", err)
	}
	generated, err := format(b.Bytes())
	if err != nil {
		log.Fatalf("formatting output: %s", err)
	}
	err = ioutil.WriteFile(tmpl[:len(tmpl)-len(".tmpl")], generated, os.ModePerm)
	if err != nil {
		log.Fatalf("writing output: %s", err)
	}
}

func format(in []byte) ([]byte, error) {
	r := bytes.NewReader(in)
	cmd := exec.Command("goimports")
	cmd.Stdin = r
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("error running goimports: %s: %s", string(out), err)
	}

	return out, nil
}
