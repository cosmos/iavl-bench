package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	pruningtypes "cosmossdk.io/store/pruning/types"
	"github.com/spf13/cobra"
	"github.com/tidwall/jsonc"

	"github.com/cosmos/iavl-bench/bench"
)

type Plan struct {
	Runs        []RunPlan         `json:"configs"`
	Simulations []bench.SimParams `json:"simulations,omitempty"`
}

type RunPlan struct {
	RunName string                       `json:"name"`
	Runner  string                       `json:"runner"`
	Options json.RawMessage              `json:"options"`
	Pruning *pruningtypes.PruningOptions `json:"pruning,omitempty"`
}

func main() {
	var dryRun bool
	var outDir string
	var leaveData bool
	cmd := &cobra.Command{
		Use:   "bench-all [plan-file]",
		Short: "Run all benchmarks in the given JSON/JSONC plan file.",
		Args:  cobra.ExactArgs(1),
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, the plan will be printed but not executed.")
	cmd.Flags().StringVar(&outDir, "out-dir", "", "If set, the directory to write results to. Defaults to a timestamped directory next to the plan file.")
	cmd.Flags().BoolVar(&leaveData, "leave-data", false, "If true, the temporary data directories will not be removed after each run.")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		planFile := args[0]
		bz, err := os.ReadFile(planFile)
		if err != nil {
			return fmt.Errorf("error reading plan file: %w", err)
		}

		var plan Plan
		err = json.Unmarshal(jsonc.ToJSON(bz), &plan)
		if err != nil {
			return fmt.Errorf("error unmarshaling plan file: %w", err)
		}

		logger := slog.Default()

		if outDir == "" {
			outDir = filepath.Join(filepath.Dir(planFile), fmt.Sprintf("run-%s", time.Now().Format("20060102_150405")))
			outDir, err = filepath.Abs(outDir)
			if err != nil {
				return fmt.Errorf("error getting absolute path of result dir: %w", err)
			}
			logger.Info(fmt.Sprintf("writing results to %s", outDir))
		}

		if !dryRun {
			err = os.MkdirAll(outDir, 0755)
			if err != nil {
				return fmt.Errorf("error creating result dir: %w", err)
			}
		}

		for _, run := range plan.Runs {
			for _, sim := range plan.Simulations {
				runOne(logger, run, sim, outDir, dryRun, leaveData)
			}
		}

		return nil
	}
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func runOne(logger *slog.Logger, plan RunPlan, simPlan bench.SimParams, resultDir string, dryRun, leaveData bool) {
	cfgBz, err := json.Marshal(plan)
	if err != nil {
		logger.Error("error marshaling plan", "error", err)
		return
	}

	simBz, err := json.Marshal(simPlan)
	if err != nil {
		logger.Error("error marshaling sim plan", "error", err)
		return
	}

	logger.Info("starting run", "config", string(cfgBz), "simulation", string(simBz))
	dir := filepath.Join(resultDir, fmt.Sprintf("%s__%s-tmp", plan.RunName, simPlan.Name))
	if !dryRun {
		err = os.Mkdir(dir, 0700)
		if err != nil {
			logger.Error("error creating db dir", "error", err)
			return
		}
		if !leaveData {
			defer os.RemoveAll(dir)
		}
	}

	args := []string{
		"bench",
		"--gen-options",
		string(simBz),
		"--db-dir",
		dir,
		"--log-type",
		"json",
		"--log-file",
		filepath.Join(resultDir, fmt.Sprintf("%s__%s.jsonl", plan.RunName, simPlan.Name)),
	}

	if plan.Options != nil {
		args = append(args, "--db-options", string(plan.Options))
	}

	if plan.Pruning != nil {
		jsonBz, err := json.Marshal(plan.Pruning)
		if err != nil {
			logger.Error("error marshaling pruning options", "error", err)
		}
		args = append(args, "--pruning-options", string(jsonBz))
	}

	cmd := exec.Command(plan.Runner, args...)
	logger.Info("executing runner command", "cmd", cmd.String())
	if dryRun {
		logger.Info("dry run, not executing command")
		return
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("error running benchmark", "error", err, "output", string(out))
		return
	}
	logger.Info("done")
}
