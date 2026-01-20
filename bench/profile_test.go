package bench

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestProfilingSession_ProfileAllWritesFiles(t *testing.T) {
	dir := t.TempDir()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	s, err := startProfiling(logger, profilingConfig{
		ProfileDir:           dir,
		ProfileAll:           true,
		MemProfileRate:       1024,
		BlockProfileRate:     1,
		MutexProfileFraction: 1,
	})
	if err != nil {
		t.Fatalf("start profiling: %v", err)
	}
	if s == nil {
		t.Fatalf("expected profiling session")
	}

	// Generate a bit of CPU and some mutex contention so the profiles aren't empty.
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50_000; j++ {
				mu.Lock()
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if err := s.stop(logger); err != nil {
		t.Fatalf("stop profiling: %v", err)
	}

	want := []string{
		"cpu.pprof",
		"heap.pprof",
		"allocs.pprof",
		"block.pprof",
		"mutex.pprof",
		"trace.out",
		"goroutine.pprof",
		"threadcreate.pprof",
	}
	for _, name := range want {
		path := filepath.Join(dir, name)
		st, err := os.Stat(path)
		if err != nil {
			t.Fatalf("missing profile %q: %v", name, err)
		}
		if st.Size() == 0 {
			t.Fatalf("profile %q is empty", name)
		}
	}
}
