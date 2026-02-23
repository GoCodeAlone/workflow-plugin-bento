package internal

import (
	"log/slog"
	"os"
)

// bentoLogger is a structured logger wrapper for all bento plugin modules.
// It uses log/slog from the standard library and attaches component context
// fields (module type, name) to every log line.
type bentoLogger struct {
	logger *slog.Logger
}

// newLogger creates a bentoLogger for the given component type and name.
// It uses the default slog handler unless one has been configured globally.
func newLogger(component, name string) *bentoLogger {
	base := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	return &bentoLogger{
		logger: base.With(
			slog.String("component", component),
			slog.String("name", name),
		),
	}
}

// LogStreamStart emits an info-level log line when a stream starts.
func (l *bentoLogger) LogStreamStart(transport string, extraFields ...any) {
	args := append([]any{slog.String("transport", transport)}, extraFields...)
	l.logger.Info("stream started", args...)
}

// LogStreamStop emits an info-level log line when a stream stops.
// uptimeSeconds is the duration the stream was running.
func (l *bentoLogger) LogStreamStop(messagesProcessed int64, extraFields ...any) {
	args := append([]any{slog.Int64("messages_processed", messagesProcessed)}, extraFields...)
	l.logger.Info("stream stopped", args...)
}

// LogStreamError emits an error-level log line for a stream error.
func (l *bentoLogger) LogStreamError(err error, extraFields ...any) {
	args := append([]any{slog.Any("error", err)}, extraFields...)
	l.logger.Error("stream error", args...)
}

// LogMessageProcessed emits a debug-level log line each time a message is processed.
func (l *bentoLogger) LogMessageProcessed(topic string, extraFields ...any) {
	args := append([]any{slog.String("topic", topic)}, extraFields...)
	l.logger.Debug("message processed", args...)
}

// LogTopicEvent emits an info-level log line for topic lifecycle events
// (creation, publish, subscribe, etc.).
func (l *bentoLogger) LogTopicEvent(event, topic string, extraFields ...any) {
	args := append([]any{
		slog.String("event", event),
		slog.String("topic", topic),
	}, extraFields...)
	l.logger.Info("topic event", args...)
}

// LogProcessingStart emits a debug-level log line when a processor step begins.
func (l *bentoLogger) LogProcessingStart(stepName string, extraFields ...any) {
	args := append([]any{slog.String("step", stepName)}, extraFields...)
	l.logger.Debug("processing started", args...)
}

// LogProcessingComplete emits a debug-level log line when a processor step finishes.
func (l *bentoLogger) LogProcessingComplete(stepName string, extraFields ...any) {
	args := append([]any{slog.String("step", stepName)}, extraFields...)
	l.logger.Debug("processing complete", args...)
}

// LogProcessingError emits an error-level log line when a processor step fails.
func (l *bentoLogger) LogProcessingError(stepName string, err error, extraFields ...any) {
	args := append([]any{
		slog.String("step", stepName),
		slog.Any("error", err),
	}, extraFields...)
	l.logger.Error("processing error", args...)
}
