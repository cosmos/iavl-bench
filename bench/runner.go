package bench

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"time"

	storev1beta1 "cosmossdk.io/api/cosmos/store/v1beta1"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protodelim"
)

// Tree is a generic interface wrapping a multi-store tree structure.
type Tree interface {
	// Version should return the last committed version. If no version has been committed, it should return 0.
	Version() int64
	// ApplyUpdate should apply a single set or delete to the tree.
	ApplyUpdate(storeKey string, key, value []byte, delete bool) error
	// Commit should persist all changes made since the last commit and return the new version's hash.
	Commit() error
}

type LoaderParams struct {
	TreeDir     string
	TreeOptions string
	StoreNames  []string
}

type TreeLoader func(params LoaderParams) (Tree, error)

type RunConfig struct {
	TreeLoader      TreeLoader
	OptionsHelpText string
}

func Run(cfg RunConfig) {
	var treeDir string
	var treeOptions string
	var changesetDir string
	var targetVersion int64
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Runs benchmarks for the tree implementation.",
	}
	cmd.Flags().StringVar(&treeDir, "tree-dir", "", "Directory for the tree's data.")
	if cfg.OptionsHelpText != "" {
		cmd.Flags().StringVar(&treeOptions, "tree-options", "", cfg.OptionsHelpText)
	}
	cmd.Flags().StringVar(&changesetDir, "changeset-dir", "", "Directory containing the changeset files.")
	cmd.Flags().Int64Var(&targetVersion, "versions", 0, "Number of versions to apply. If this is empty or 0, all versions in the changeset-dir will be applied.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if treeDir == "" {
			return fmt.Errorf("tree-dir is required")
		}

		if changesetDir == "" {
			return fmt.Errorf("changeset-dir is required")
		}

		info, err := readInfoFile(changesetDir)
		if err != nil {
			return fmt.Errorf("error reading changeset info file: %w", err)
		}

		if targetVersion <= 0 {
			targetVersion = info.Versions
		}

		loaderParams := LoaderParams{
			TreeDir:     treeDir,
			TreeOptions: treeOptions,
			StoreNames:  info.StoreNames,
		}

		tree, err := cfg.TreeLoader(loaderParams)
		if err != nil {
			return fmt.Errorf("error loading tree: %w", err)
		}

		return run(tree, changesetDir, runParams{
			TargetVersion: targetVersion,
			Logger:        slog.Default(),
		})
	}

	err := cmd.Execute()
	if err != nil {
		slog.Error("error running benchmarks", "error", err)
	}
}

type runParams struct {
	TargetVersion int64
	Logger        *slog.Logger
}

func run(tree Tree, changesetDir string, params runParams) error {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}
	version := tree.Version()
	target := params.TargetVersion
	logger.Info("starting run", "start_version", version, "target_version", target)
	for version < target {
		version++
		err := applyVersion(logger, tree, changesetDir, version)
		if err != nil {
			return fmt.Errorf("error applying version %d: %w", version, err)
		}
	}
	return nil
}

func applyVersion(logger *slog.Logger, tree Tree, dataDir string, version int64) error {
	dataFilename := dataFilename(dataDir, version)
	dataFile, err := os.Open(dataFilename)
	if err != nil {
		return fmt.Errorf("error opening changeset file for version %d: %w", version)
	}
	defer func() {
		err := dataFile.Close()
		if err != nil {
			panic(err)
		}
	}()
	reader := bufio.NewReader(dataFile)

	logger.Info("applying changeset", "version", version, "file", dataFilename)
	i := 0
	startTime := time.Now()
	for {
		if i%10_000 == 0 && i > 0 {
			logger.Debug("applied changes", "version", version, "count", i)
		}
		var storeKVPair storev1beta1.StoreKVPair
		err := protodelim.UnmarshalFrom(reader, &storeKVPair)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error at entry %d reading changeset: %w", i, err)
		}

		err = tree.ApplyUpdate(storeKVPair.StoreKey, storeKVPair.Key, storeKVPair.Value, storeKVPair.Delete)
		if err != nil {
			return fmt.Errorf("error at entry %d applying update: %w", i, err)
		}

		i++
	}
	logger.Info("applied all changes, commiting", "version", version, "count", i)

	err = tree.Commit()
	if err != nil {
		return fmt.Errorf("error committing version %d: %w", version, err)
	}

	duration := time.Since(startTime)
	opsPerSec := float64(i) / duration.Seconds()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	logger.Info(
		"committed version",
		"version", version,
		"duration", duration,
		"ops_per_sec", opsPerSec,
		"mem_allocs", humanize.Bytes(memStats.Alloc),
		"mem_sys", humanize.Bytes(memStats.Sys),
		"mem_num_gc", humanize.Comma(int64(memStats.NumGC)),
	)

	return nil
}
