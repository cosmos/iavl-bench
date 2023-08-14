package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kocubinski/iavl-bench/iavl-v2/cmd"

	"github.com/kocubinski/iavl-bench/core"
)

func main() {
	root, err := core.RootCommand()
	if err != nil {
		os.Exit(1)
	}

	root.AddCommand(cmd.TreeCommand(context.Background()))

	if err := root.Execute(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}
