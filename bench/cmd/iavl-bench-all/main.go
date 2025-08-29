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
)

type Plan struct {
	ChangesetDir string    `json:"changeset_dir"`
	Versions     int64     `json:"versions"`
	Runs         []RunPlan `json:"runs"`
}

type RunPlan struct {
	RunName      string          `json:"name"`
	Runner       string          `json:"runner"`
	Options      json.RawMessage `json:"options"`
	ChangesetDir string          `json:"changeset_dir"`
	Versions     int64           `json:"versions"`
}

func main() {
	var dryRun bool
	cmd := &cobra.Command{
		Use:  "bench-all [plan-file]",
		Args: cobra.ExactArgs(1),
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, the plan will be printed but not executed.")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		planFile := args[0]
		bz, err := os.ReadFile(planFile)
		if err != nil {
			return fmt.Errorf("error reading plan file: %w", err)
		}

		var plan Plan
		err = json.Unmarshal(bz, &plan)
		if err != nil {
			return fmt.Errorf("error unmarshaling plan file: %w", err)
		}

		logger := slog.Default()

		resultDir := filepath.Join(filepath.Dir(planFile), time.Now().Format("20060102_150405"))
		resultDir, err = filepath.Abs(resultDir)
		if err != nil {
			return fmt.Errorf("error getting absolute path of result dir: %w", err)
		}
		logger.Info(fmt.Sprintf("writing results to %s", resultDir))

		if !dryRun {
			err = os.MkdirAll(resultDir, 0755)
			if err != nil {
				return fmt.Errorf("error creating result dir: %w", err)
			}
		}

		for _, run := range plan.Runs {
			// fill in defaults from plan
			if run.ChangesetDir == "" {
				run.ChangesetDir = plan.ChangesetDir
			}
			if run.Versions == 0 {
				run.Versions = plan.Versions
			}
			runOne(logger, run, resultDir, dryRun)
		}

		return nil
	}
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func runOne(logger *slog.Logger, plan RunPlan, resultDir string, dryRun bool) {
	bz, err := json.Marshal(plan)
	if err != nil {
		logger.Error("error marshaling plan", "error", err)
		return
	}
	logger.Info("starting run", "run_plan", string(bz))
	dir, err := os.MkdirTemp("", plan.Runner)
	if err != nil {
		logger.Error("error creating db dir", "error", err)
		return
	}
	defer os.RemoveAll(dir)

	args := []string{
		"bench",
		"--changeset-dir",
		plan.ChangesetDir,
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

	if plan.Versions != 0 {
		args = append(args, "--target-version", fmt.Sprintf("%d", plan.Versions))
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
