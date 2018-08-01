package main

import (
	"os"
	"github.com/getsentry/raven-go"
)

func init() {
	raven.SetDSN(RavenDsn)
}

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "--insert" {
			insertMain()
		} else if os.Args[1] == "--entity" {
			entityMain()
		} else if os.Args[1] == "--mark" {
			markMain()
		} else if os.Args[1] == "--invite" {
			inviteMain()
		}
	}
}
