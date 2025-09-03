package util

import (
	"log/slog"
	"runtime"
	"time"

	"cosmossdk.io/log"
)

// SlogWrapper wraps a slog.Logger to implement cosmossdk.io/log's Logger interface
// while preserving proper source code location information
type SlogWrapper struct {
	logger *slog.Logger
}

// NewSlogWrapper creates a new SlogWrapper from an slog.Logger
func NewSlogWrapper(logger *slog.Logger) *SlogWrapper {
	return &SlogWrapper{logger: logger}
}

var _ log.Logger = &SlogWrapper{}

func (w *SlogWrapper) Debug(msg string, keyvals ...interface{}) {
	if !w.logger.Enabled(nil, slog.LevelDebug) {
		return
	}
	w.logWithSource(slog.LevelDebug, msg, keyvals...)
}

// Info logs at info level with proper source location
func (w *SlogWrapper) Info(msg string, keyvals ...interface{}) {
	if !w.logger.Enabled(nil, slog.LevelInfo) {
		return
	}
	w.logWithSource(slog.LevelInfo, msg, keyvals...)
}

// Error logs at error level with proper source location
func (w *SlogWrapper) Error(msg string, keyvals ...interface{}) {
	if !w.logger.Enabled(nil, slog.LevelError) {
		return
	}
	w.logWithSource(slog.LevelError, msg, keyvals...)
}

func (w *SlogWrapper) Warn(msg string, keyVals ...any) {
	if !w.logger.Enabled(nil, slog.LevelWarn) {
		return
	}
	w.logWithSource(slog.LevelWarn, msg, keyVals...)
}

func (w *SlogWrapper) With(keyVals ...any) log.Logger {
	return &SlogWrapper{
		logger: w.logger.With(keyVals...),
	}
}

func (w *SlogWrapper) Impl() any {
	return w.logger
}

// logWithSource logs with the correct source location by skipping wrapper frames
func (w *SlogWrapper) logWithSource(level slog.Level, msg string, keyvals ...interface{}) {
	// skip frames: this function (1) + the calling method (Debug/Info/Error) (1) + the caller (1) = 3
	pc, _, _, ok := runtime.Caller(3)
	if !ok {
		// fallback if we can't get caller info
		w.logger.Log(nil, level, msg, keyvals...)
		return
	}

	// create a record with the correct source location and current time
	record := slog.NewRecord(time.Now(), level, msg, pc)
	record.Add(keyvals...)

	// handle the record
	_ = w.logger.Handler().Handle(nil, record)
}
