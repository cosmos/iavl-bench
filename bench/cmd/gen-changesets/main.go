package main

import (
	"fmt"
	"math/rand/v2"

	"github.com/spf13/cobra"

	"github.com/cosmos/iavl-bench/bench"
)

func MixedGenerators(versions int64, scale float64) []bench.StoreParams {
	gens := []bench.StoreParams{
		bench.BankLikeGenerator(versions, scale),
		bench.StakingLikeGenerator(versions, scale),
		bench.LockupLikeGenerator(versions, scale),
	}
	return gens
}

func BankV2(accounts int) []bench.StoreParams {
	return []bench.StoreParams{
		{
			StoreKey:         "bank",
			KeyMean:          45, // a key is a 1 byte prefix + 32 byte address + denom (5-20 bytes)
			KeyStdDev:        4,
			ValueMean:        12, // a value is simply a formatted integer string (roughly 8-16 bytes)
			ValueStdDev:      2,
			InitialSize:      accounts,
			FinalSize:        accounts + 1000, // small growth over time of only 1k accounts
			Versions:         2000,            // 2000 versions
			ChangePerVersion: 10_000,          // 10k changes per version which represents 5k swaps per block
			DeleteFraction:   0.01,            // we're only deleting 1% of accounts
		},
	}
}

func main() {
	var versions int64
	var profile string
	var scale float64
	cmd := &cobra.Command{
		Use:   "gen-changesets [out-dir]",
		Short: "Generate changesets for iavl-bench",
		Args:  cobra.ExactArgs(1),
	}
	cmd.Flags().Int64Var(&versions, "versions", 100, "number of versions to generate")
	cmd.Flags().StringVar(&profile, "profile", "mixed", "data generation profile to use (mixed|osmo|bank1m); default is mixed")
	cmd.Flags().Float64Var(&scale, "scale", 1.0, "float64 scale factor for the profile; default is 1.0")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		var gens []bench.StoreParams
		switch profile {
		case "mixed":
			gens = MixedGenerators(versions, scale)
		case "osmo":
			gens = bench.OsmoLikeGenerators(scale)
		case "bank1m":
			gens = BankV2(1_000_000)
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
	}
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
