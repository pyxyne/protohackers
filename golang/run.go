package main

import (
	"fmt"
	"os"
)

var scripts = map[string]func(){
	"P0": P0,
	"P1": P1,
	"P2": P2,
	"P3": P3,
	"P4": P4,
	"P5": P5,
	"P6": P6,
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Expected 1 command line argument: script to run")
		os.Exit(1)
	}
	script, ok := scripts[os.Args[1]]
	if !ok {
		fmt.Printf("No script with name %q\n", os.Args[1])
		os.Exit(1)
	}
	fmt.Printf("Running script %s\n", os.Args[1])
	script()
}
