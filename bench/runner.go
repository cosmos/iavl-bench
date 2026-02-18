package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"

	storetypes "cosmossdk.io/store/types"
	"github.com/spf13/cobra"
)

type LoaderParams struct {
	TreeDir     string
	TreeOptions interface{}
	StoreKeys   []*storetypes.KVStoreKey
	Logger      *slog.Logger
}

type TreeLoader func(params LoaderParams) (RootMultiTree, error)

type RunConfig struct {
	TreeLoader  TreeLoader
	OptionsType interface{}
}

func Run(treeType string, cfg RunConfig) {
	NewRunner(treeType, cfg).Run()
}

type Runner struct {
	*cobra.Command
}

func (r Runner) Run() {
	err := r.Command.Execute()
	if err != nil {
		slog.Error("error running benchmarks", "error", err)
		os.Exit(1)
	}
}

func NewRunner(treeType string, cfg RunConfig) Runner {
	var treeDir string
	var treeOptions string
	var pruningOptions string
	var genOptions string
	var logHandlerType string
	var logFile string
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Runs benchmarks for the tree implementation.",
	}
	cmd.Flags().StringVar(&treeDir, "db-dir", "", "Directory for the db's data.")
	cmd.Flags().StringVar(&treeOptions, "db-options", "", "Implementation specific options for the db, in JSON format.")
	cmd.Flags().StringVar(&pruningOptions, "pruning-options", "", "Pruning options, in JSON format.")
	cmd.Flags().StringVar(&genOptions, "gen-options", "", "Changeset generator params, in JSON format.")
	cmd.Flags().StringVar(&logHandlerType, "log-type", "text", "Log handler type. One of 'text' or 'json'.")
	cmd.Flags().StringVar(&logFile, "log-file", "", "If set, log output will be written to this file instead of stdout.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if treeDir == "" {
			return fmt.Errorf("tree-dir is required")
		}

		var genParams SimParams
		if genOptions == "" {
			return fmt.Errorf("gen-options is required")
		}
		decoder := json.NewDecoder(bytes.NewReader([]byte(genOptions)))
		// we disallow unknown fields to catch typos with generator options
		decoder.DisallowUnknownFields()
		err := decoder.Decode(&genParams)
		if err != nil {
			return fmt.Errorf("error unmarshaling gen-options: %w", err)
		}

		// decode db options from json
		var parsedOpts interface{}
		if cfg.OptionsType != nil {
			parsedOpts = reflect.New(reflect.TypeOf(cfg.OptionsType).Elem()).Interface()
			if treeOptions != "" {
				if cfg.OptionsType == nil {
					return fmt.Errorf("db-options provided but no OptionsType set in RunConfig")
				}
				decoder := json.NewDecoder(bytes.NewReader([]byte(treeOptions)))
				// we disallow unknown fields to catch typos with database options
				decoder.DisallowUnknownFields()
				err := decoder.Decode(parsedOpts)
				if err != nil {
					return fmt.Errorf("error unmarshaling db-options: %w", err)
				}
			}
		}

		logOut := os.Stdout
		if logFile != "" {
			logOut, err = os.Create(logFile)
			if err != nil {
				return fmt.Errorf("error creating log file: %w", err)
			}
			defer func() {
				err := logOut.Close()
				if err != nil {
					slog.Error("error closing log file", "error", err)
				}
			}()
		}

		var handler slog.Handler
		switch logHandlerType {
		case "text":
			handler = slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug})
		case "json":
			handler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		// Create a separate handler for tree logger at info level
		var treeHandler slog.Handler
		switch logHandlerType {
		case "text":
			treeHandler = slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo})
		case "json":
			treeHandler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo, AddSource: true})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		logger := slog.New(handler).With("module", "runner")
		treeLogger := slog.New(treeHandler).With("module", treeType)
		logger.Info("Starting benchmark run, loading tree")

		return RunSimulation(logger, TreeParams{
			TreeLogger:     treeLogger,
			TreeLoader:     cfg.TreeLoader,
			TreeDir:        treeDir,
			TreeOptions:    parsedOpts,
			TreeType:       treeType,
			PruningOptions: pruningOptions,
		}, genParams)
	}

	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(cmd)
	return Runner{Command: rootCmd}
}
