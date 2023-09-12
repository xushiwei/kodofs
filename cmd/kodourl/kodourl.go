//go:build ignore
// +build ignore

// kodourl is an example for creating a protected kodofs url.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/qiniu/x/token/protected"
)

func init() {
	protected.KeySalt = "<Please use your RandomKeySalt>"
	protected.EnvKeyName = "<Please input the environment variable name of a RandomKey>"
}

var (
	help = flag.Bool("h", false, "show this help information")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "kodourl [flags] <bucket> <ak> <sk>\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if *help || len(args) != 3 {
		flag.Usage()
		return
	}
	params := make(url.Values)
	params.Set("ak", args[1])
	params.Set("sk", args[2])
	query, err := protected.Encode(params)
	check(err)

	fmt.Printf("kodo:%s?%s\n", args[0], query)
}

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
