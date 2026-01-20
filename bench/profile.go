package bench

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"sync"
	"time"
)

type profilingConfig struct {
	ProfileDir    string
	ProfilePrefix string
	ProfileAll    bool

	CPUProfile          string
	MemProfile          string
	AllocsProfile       string
	BlockProfile        string
	MutexProfile        string
	TraceProfile        string
	GoroutineProfile    string
	ThreadcreateProfile string

	MemProfileRate       int
	BlockProfileRate     int
	MutexProfileFraction int

	PprofHTTP string
}

type profilingSession struct {
	cfg profilingConfig

	once sync.Once

	cpuFile   *os.File
	traceFile *os.File

	httpLn  net.Listener
	httpSrv *http.Server

	restoreMemRate int
}

func startProfiling(logger *slog.Logger, cfg profilingConfig) (*profilingSession, error) {
	enabled := cfg.ProfileAll ||
		cfg.CPUProfile != "" ||
		cfg.MemProfile != "" ||
		cfg.AllocsProfile != "" ||
		cfg.BlockProfile != "" ||
		cfg.MutexProfile != "" ||
		cfg.TraceProfile != "" ||
		cfg.GoroutineProfile != "" ||
		cfg.ThreadcreateProfile != "" ||
		cfg.PprofHTTP != "" ||
		cfg.MemProfileRate > 0 ||
		cfg.BlockProfileRate > 0 ||
		cfg.MutexProfileFraction > 0
	if !enabled {
		return nil, nil
	}

	if cfg.ProfileAll {
		// Default profile outputs require a base directory.
		cfg.CPUProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.CPUProfile, "cpu.pprof")
		cfg.MemProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.MemProfile, "heap.pprof")
		cfg.AllocsProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.AllocsProfile, "allocs.pprof")
		cfg.BlockProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.BlockProfile, "block.pprof")
		cfg.MutexProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.MutexProfile, "mutex.pprof")
		cfg.TraceProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.TraceProfile, "trace.out")
		cfg.GoroutineProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.GoroutineProfile, "goroutine.pprof")
		cfg.ThreadcreateProfile = defaultProfilePath(cfg.ProfileDir, cfg.ProfilePrefix, cfg.ThreadcreateProfile, "threadcreate.pprof")
	}

	if cfg.ProfileDir != "" {
		cfg.CPUProfile = joinProfileDir(cfg.ProfileDir, cfg.CPUProfile)
		cfg.MemProfile = joinProfileDir(cfg.ProfileDir, cfg.MemProfile)
		cfg.AllocsProfile = joinProfileDir(cfg.ProfileDir, cfg.AllocsProfile)
		cfg.BlockProfile = joinProfileDir(cfg.ProfileDir, cfg.BlockProfile)
		cfg.MutexProfile = joinProfileDir(cfg.ProfileDir, cfg.MutexProfile)
		cfg.TraceProfile = joinProfileDir(cfg.ProfileDir, cfg.TraceProfile)
		cfg.GoroutineProfile = joinProfileDir(cfg.ProfileDir, cfg.GoroutineProfile)
		cfg.ThreadcreateProfile = joinProfileDir(cfg.ProfileDir, cfg.ThreadcreateProfile)
	}

	s := &profilingSession{cfg: cfg}
	if err := s.start(logger); err != nil {
		_ = s.stop(logger)
		return nil, err
	}
	return s, nil
}

func (s *profilingSession) start(logger *slog.Logger) error {
	if s.cfg.ProfileDir != "" {
		if err := os.MkdirAll(s.cfg.ProfileDir, 0o755); err != nil {
			return fmt.Errorf("create profile dir: %w", err)
		}
	}

	if s.cfg.MemProfileRate > 0 && runtime.MemProfileRate != s.cfg.MemProfileRate {
		s.restoreMemRate = runtime.MemProfileRate
		runtime.MemProfileRate = s.cfg.MemProfileRate
	}
	if s.cfg.BlockProfileRate > 0 {
		runtime.SetBlockProfileRate(s.cfg.BlockProfileRate)
	}
	if s.cfg.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(s.cfg.MutexProfileFraction)
	}

	if s.cfg.PprofHTTP != "" {
		ln, err := net.Listen("tcp", s.cfg.PprofHTTP)
		if err != nil {
			return fmt.Errorf("pprof http listen: %w", err)
		}
		s.httpLn = ln
		s.httpSrv = &http.Server{
			ReadHeaderTimeout: 5 * time.Second,
		}
		if logger != nil {
			logger.Info("pprof http server listening", "addr", ln.Addr().String())
		}
		go func() { _ = s.httpSrv.Serve(ln) }()
	}

	if s.cfg.CPUProfile != "" {
		f, err := createFile(s.cfg.CPUProfile)
		if err != nil {
			return fmt.Errorf("create cpu profile: %w", err)
		}
		s.cpuFile = f
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			s.cpuFile = nil
			return fmt.Errorf("start cpu profile: %w", err)
		}
	}

	if s.cfg.TraceProfile != "" {
		f, err := createFile(s.cfg.TraceProfile)
		if err != nil {
			return fmt.Errorf("create trace: %w", err)
		}
		s.traceFile = f
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			s.traceFile = nil
			return fmt.Errorf("start trace: %w", err)
		}
	}

	return nil
}

func (s *profilingSession) stop(logger *slog.Logger) (retErr error) {
	s.once.Do(func() {
		defer func() {
			if s.restoreMemRate != 0 {
				runtime.MemProfileRate = s.restoreMemRate
			}
			if s.cfg.BlockProfileRate > 0 {
				runtime.SetBlockProfileRate(0)
			}
			if s.cfg.MutexProfileFraction > 0 {
				runtime.SetMutexProfileFraction(0)
			}
		}()

		var errs []error

		if s.traceFile != nil {
			trace.Stop()
			errs = append(errs, s.traceFile.Close())
			s.traceFile = nil
		}
		if s.cpuFile != nil {
			pprof.StopCPUProfile()
			errs = append(errs, s.cpuFile.Close())
			s.cpuFile = nil
		}

		writeProfile := func(path string, name string, gcFirst bool) {
			if path == "" {
				return
			}
			prof := pprof.Lookup(name)
			if prof == nil {
				errs = append(errs, fmt.Errorf("missing profile %q", name))
				return
			}
			if gcFirst {
				runtime.GC()
			}
			f, err := createFile(path)
			if err != nil {
				errs = append(errs, fmt.Errorf("create %s profile: %w", name, err))
				return
			}
			defer func() { errs = append(errs, f.Close()) }()
			if err := prof.WriteTo(f, 0); err != nil {
				errs = append(errs, fmt.Errorf("write %s profile: %w", name, err))
			}
		}

		writeProfile(s.cfg.MemProfile, "heap", true)
		writeProfile(s.cfg.AllocsProfile, "allocs", true)
		writeProfile(s.cfg.BlockProfile, "block", false)
		writeProfile(s.cfg.MutexProfile, "mutex", false)
		writeProfile(s.cfg.GoroutineProfile, "goroutine", false)
		writeProfile(s.cfg.ThreadcreateProfile, "threadcreate", false)

		if s.httpSrv != nil {
			_ = s.httpSrv.Close()
			s.httpSrv = nil
		}
		if s.httpLn != nil {
			_ = s.httpLn.Close()
			s.httpLn = nil
		}

		retErr = errors.Join(errs...)
		if retErr != nil && logger != nil {
			logger.Error("profiling teardown errors", "error", retErr)
		}
	})
	return retErr
}

func defaultProfilePath(dir, prefix, explicit, baseName string) string {
	if explicit != "" {
		return explicit
	}
	if dir == "" {
		return ""
	}
	if prefix != "" && !strings.HasSuffix(prefix, "-") {
		prefix += "-"
	}
	return prefix + baseName
}

func joinProfileDir(dir, path string) string {
	if dir == "" || path == "" {
		return path
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
}

func createFile(path string) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("empty path")
	}
	parent := filepath.Dir(path)
	if parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return nil, err
		}
	}
	return os.Create(path)
}
