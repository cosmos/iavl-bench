package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/tidwall/jsonc"
)

type Plan struct {
	Runs []RunPlan `json:"runs"`
}

type RunPlan struct {
	RunName string          `json:"name"`
	Runner  string          `json:"runner"`
	Options json.RawMessage `json:"options"`
}

func main() {
	var dryRun bool
	var changesetDir string
	var versions int64
	var outDir string
	cmd := &cobra.Command{
		Use:   "bench-all [plan-file]",
		Short: "Run all benchmarks in the given JSON/JSONC plan file.",
		Args:  cobra.ExactArgs(1),
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, the plan will be printed but not executed.")
	cmd.Flags().StringVar(&changesetDir, "changeset-dir", "", "Directory containing changesets.")
	cmd.Flags().Int64Var(&versions, "target-version", 0, "If non-zero, the target version to run the benchmarks against.")
	cmd.Flags().StringVar(&outDir, "out-dir", "", "If set, the directory to write results to. Defaults to a timestamped directory next to the plan file.")
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
			runOne(logger, run, changesetDir, versions, outDir, dryRun)
		}

		return nil
	}
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func runOne(logger *slog.Logger, plan RunPlan, changesetDir string, versions int64, resultDir string, dryRun bool) {
	bz, err := json.Marshal(plan)
	if err != nil {
		logger.Error("error marshaling plan", "error", err)
		return
	}
	logger.Info("starting run", "run_plan", string(bz))
	dir := filepath.Join(resultDir, fmt.Sprintf("%s-tmp", plan.RunName))
	err = os.Mkdir(dir, 0700)
	if err != nil {
		logger.Error("error creating db dir", "error", err)
		return
	}
	defer os.RemoveAll(dir)

	args := []string{
		"bench",
		"--changeset-dir",
		changesetDir,
		"--db-dir",
		dir,
		"--log-type",
		"json",
		"--log-file",
		filepath.Join(resultDir, fmt.Sprintf("%s.jsonl", plan.RunName)),
	}

	if plan.Options != nil {
		args = append(args, "--db-options", string(plan.Options))
	}

	if versions != 0 {
		args = append(args, "--target-version", fmt.Sprintf("%d", versions))
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
