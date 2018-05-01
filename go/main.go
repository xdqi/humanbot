package main

import (
	"os"
)

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "--insert" {
			insertMain()
		} else if os.Args[1] == "--entity" {
			entityMain()
		}
	}
}
