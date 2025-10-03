package x3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type ChangesetFiles struct {
	dir          string
	kvlogPath    string
	kvlogFile    *os.File
	branchesFile *os.File
	leavesFile   *os.File
	versionsFile *os.File
	infoFile     *os.File
	info         *ChangesetInfo
	infoMmap     *StructMmap[ChangesetInfo]
}

func OpenChangesetFiles(dir, kvlogPath string) (*ChangesetFiles, error) {
	// ensure absolute path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for %s: %w", dir, err)
	}
	dir = absDir

	err = os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create changeset dir: %w", err)
	}

	if kvlogPath == "" {
		kvlogPath = filepath.Join(dir, "kv.log")
	}
	kvlogFile, err := os.OpenFile(kvlogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create KV log file: %w", err)
	}

	leavesPath := filepath.Join(dir, "leaves.dat")
	leavesFile, err := os.OpenFile(leavesPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create leaves data file: %w", err)
	}

	branchesPath := filepath.Join(dir, "branches.dat")
	branchesFile, err := os.OpenFile(branchesPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create branches data file: %w", err)
	}

	versionsPath := filepath.Join(dir, "versions.dat")
	versionsFile, err := os.OpenFile(versionsPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create versions data file: %w", err)
	}

	infoPath := filepath.Join(dir, "info.dat")
	infoFile, err := os.OpenFile(infoPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create changeset info file: %w", err)
	}

	// check file size to see if we need to initialize
	stat, err := infoFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat info file: %w", err)
	}

	if stat.Size() == 0 {
		// file is empty, initialize it
		infoWriter := NewStructWriter[ChangesetInfo](infoFile)
		if err := infoWriter.Append(&ChangesetInfo{}); err != nil {
			return nil, fmt.Errorf("failed to write initial changeset info: %w", err)
		}
		if err := infoWriter.Flush(); err != nil {
			return nil, fmt.Errorf("failed to flush initial changeset info: %w", err)
		}
	}

	// now create the mmap reader
	infoMmap, err := NewStructReader[ChangesetInfo](infoFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open changeset info: %w", err)
	}

	if infoMmap.Count() != 1 {
		return nil, fmt.Errorf("changeset info file has unexpected item count: %d", infoMmap.Count())
	}

	return &ChangesetFiles{
		dir:          dir,
		kvlogPath:    kvlogPath,
		kvlogFile:    kvlogFile,
		branchesFile: branchesFile,
		leavesFile:   leavesFile,
		versionsFile: versionsFile,
		infoFile:     infoFile,
		info:         infoMmap.UnsafeItem(0),
		infoMmap:     infoMmap,
	}, nil
}

type ChangesetDeleteArgs struct {
	SaveKVLogPath string
}

func (cr *ChangesetFiles) Close() error {
	return errors.Join(
		cr.kvlogFile.Close(),
		cr.branchesFile.Close(),
		cr.leavesFile.Close(),
		cr.versionsFile.Close(),
		cr.infoFile.Close(),
	)
}

func (cr *ChangesetFiles) DeleteFiles(args ChangesetDeleteArgs) error {
	errs := []error{
		os.Remove(cr.infoFile.Name()),
		os.Remove(cr.leavesFile.Name()),
		os.Remove(cr.branchesFile.Name()),
		os.Remove(cr.versionsFile.Name()),
	}
	if cr.kvlogPath != args.SaveKVLogPath {
		errs = append(errs, os.Remove(cr.kvlogPath))
	}
	return errors.Join(errs...)
}
