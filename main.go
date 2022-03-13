package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	in := flag.String("input_file", "in.json", "input file")
	wsize := flag.Uint("window_size", 10, "window size")
	flag.Parse()

	f, err := os.Open(*in)
	if err != nil {
		fmt.Printf("unable to open file '%s': %s", *in, err)
		return
	}

	if err := calculateAvg(f, os.Stdout, *wsize); err != nil {
		fmt.Printf("unable to calculate average: %s", err)
		return
	}
}
