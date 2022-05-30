package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/joomcode/errorx"
	"github.com/patsak/tlvm"
)

func main() {
	flag.Parse()

	inputFile := flag.Arg(0)
	var code []byte
	var err error
	if inputFile == "-" {
		code, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
	} else {
		code, err = ioutil.ReadFile(inputFile)
		if err != nil {
			panic(err)
		}
	}

	bt, err := tlvm.Compile(string(code))
	if err != nil {
		panic(err)
	}

	vm := tlvm.NewVM(bt)
	if err := vm.Execute(); err != nil {
		errorx.Panic(err)
	}

	fmt.Println(vm.Result())
}
