package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/iavl-v2/cmd"
)

func main() {
	root, err := bench.RootCommand()
	if err != nil {
		os.Exit(1)
	}

	root.AddCommand(cmd.TreeCommand(&cmd.Context{
		TreeContext: bench.TreeContext{
			Context: context.Background(),
		},
	}))

	if err := root.Execute(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}
