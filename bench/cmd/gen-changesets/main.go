package main

import (
	"fmt"
	"math/rand/v2"

	"github.com/spf13/cobra"

	"github.com/cosmos/iavl-bench/bench"
)

//func GetDefaultGenerators() []bench.StoreParams {
//	gens := bench.OsmoLikeGenerators()
//	gens = append(gens, bench.StakingLikeGenerator(0, 1_000_000))
//	gens = append(gens, bench.LockupLikeGenerator(1, 1_000_000))
//	return gens
//}

func SmallGenerators() []bench.StoreParams {
	gens := []bench.StoreParams{
		bench.BankLikeGenerator(200_000),
		bench.StakingLikeGenerator(200_000),
		bench.LockupLikeGenerator(200_000),
	}
	return gens
}

func main() {
	var versions int64
	var profile string
	cmd := &cobra.Command{
		Use:   "gen-changesets [out-dir]",
		Short: "Generate changesets for iavl-bench",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var gens []bench.StoreParams
			switch profile {
			case "small":
				gens = SmallGenerators()
			case "osmo":
				gens = bench.OsmoLikeGenerators()
			default:
				return fmt.Errorf("unknown generator profile: %s", profile)
			}

			outDir := args[0]

			rngSource := rand.NewPCG(0, 0)
			gen := bench.TreeParams{
				StoreParams: gens,
				Versions:    versions,
				RandSource:  rngSource,
			}

			return bench.GenerateChangesets(gen, outDir)
		},
	}
	cmd.Flags().Int64Var(&versions, "versions", 100, "number of versions to generate")
	cmd.Flags().StringVar(&profile, "profile", "small", "data generation profile to use (small|osmo); default is small")
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
