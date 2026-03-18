package cdc

// Logger is a narrow structured logger contract used by CDC probe/checkpoint flow.
type Logger interface {
	Info() LogEvent
	Warn() LogEvent
	Error() LogEvent
	Debug() LogEvent
}

// LogEvent is a minimal chainable logging event contract.
type LogEvent interface {
	Str(key, val string) LogEvent
	Int(key string, val int) LogEvent
	Err(err error) LogEvent
	Msg(msg string)
}

// MetricsRecorder is a narrow CDC metrics contract used by extraction primitives.
type MetricsRecorder interface {
	RecordProbe(success bool)
	RecordEnsure(result string)
	RecordEnsureWithLatency(result string, latencySeconds float64)
}

func ensureLogger(log Logger) Logger {
	if log != nil {
		return log
	}
	return noopLogger{}
}

type noopLogger struct{}

func (noopLogger) Info() LogEvent  { return noopLogEvent{} }
func (noopLogger) Warn() LogEvent  { return noopLogEvent{} }
func (noopLogger) Error() LogEvent { return noopLogEvent{} }
func (noopLogger) Debug() LogEvent { return noopLogEvent{} }

type noopLogEvent struct{}

func (noopLogEvent) Str(string, string) LogEvent { return noopLogEvent{} }
func (noopLogEvent) Int(string, int) LogEvent    { return noopLogEvent{} }
func (noopLogEvent) Err(error) LogEvent          { return noopLogEvent{} }
func (noopLogEvent) Msg(string)                  {}
