package main

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

type Plan struct {
	RunName      string
	Runner       string
	Options      string
	ChangesetDir string
	Versions     int64
}

func main() {
	var planFile string
	var dryRun bool
	var defaultChangeset string
	cmd := &cobra.Command{
		Use: "bench-all",
	}
	cmd.Flags().StringVar(&planFile, "plan", "", "CSV file containing a plan of the benchmarks to run.")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "If true, the plan will be printed but not executed.")
	cmd.Flags().StringVar(&defaultChangeset, "default-changeset", "", "Default changeset directory to use if not specified in the plan.")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if planFile == "" {
			return fmt.Errorf("plan is required")
		}

		plans, err := readPlanFile(planFile, defaultChangeset)
		if err != nil {
			return fmt.Errorf("error reading plan file: %w", err)
		}

		logger := slog.Default()

		resultDir := filepath.Join(filepath.Base(planFile), time.Now().Format("20060102_150405"))
		err = os.MkdirAll(resultDir, 0755)
		if err != nil {
			return fmt.Errorf("error creating result dir: %w", err)
		}
		resultDir, err = filepath.Abs(resultDir)
		if err != nil {
			return fmt.Errorf("error getting absolute path of result dir: %w", err)
		}
		logger.Info(fmt.Sprintf("writing results to %s", resultDir))

		for _, plan := range plans {
			runOne(logger, plan, resultDir, dryRun)
		}

		return nil
	}
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func readPlanFile(file string, changesetDir string) ([]Plan, error) {
	planIds := make(map[string]struct{})
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("error opening plan file: %w", err)
	}
	rdr := csv.NewReader(f)
	allRows, err := rdr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading plan file: %w", err)
	}
	plans := make([]Plan, 0, len(allRows)-1)
	for i, row := range allRows {
		if i == 0 {
			// skip header
			continue
		}
		if len(row) < 5 {
			return nil, fmt.Errorf("invalid plan file: row %d has less than 5 columns", i+1)
		}
		runName := row[0]
		runner := row[1]
		options := row[2]
		if row[3] != "" {
			changesetDir = row[3]
		}
		if changesetDir == "" {
			return nil, fmt.Errorf("invalid plan file: row %d has no changeset dir and no default provided", i+1)
		}
		versions := int64(0)
		if row[4] != "" {
			_, err := fmt.Sscanf(row[4], "%d", &versions)
			if err != nil {
				return nil, fmt.Errorf("invalid plan file: row %d has invalid versions: %w", i+1, err)
			}
		}

		if _, exists := planIds[runName]; exists {
			return nil, fmt.Errorf("invalid plan file: duplicate run name %s", runName)
		}
		planIds[runName] = struct{}{}

		plans = append(plans, Plan{
			RunName:      runName,
			Runner:       runner,
			ChangesetDir: changesetDir,
			Options:      options,
			Versions:     versions,
		})
	}

	return plans, nil
}

func runOne(logger *slog.Logger, plan Plan, resultDir string, dryRun bool) {
	logger.Info("running plan", "plan", plan, "options", plan.Options)
	dir, err := os.MkdirTemp("", plan.Runner)
	if err != nil {
		logger.Error("error creating db dir", "error", err)
		return
	}
	defer os.RemoveAll(dir)

	cmd := exec.Command(
		"go",
		"run",
		"-buildvcs=true", // capture git info in the binary
		".",
		"bench",
		"--changeset-dir",
		plan.ChangesetDir,
		"--db-dir",
		dir,
		"--target-version",
		fmt.Sprintf("%d", plan.Versions),
		"--log-type",
		"json",
		"--log-file",
		filepath.Join(resultDir, fmt.Sprintf("%s.jsonl", plan.RunName)),
	)
	cmd.Dir = plan.Runner
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
