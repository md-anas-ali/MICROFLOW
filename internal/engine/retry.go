package engine

import "errors"

// permanentError marks an error as a permanent (non-transient)
// configuration/credential failure -- e.g. HTTP 401/403, a missing API
// key, an invalid grant. Retrying it with the same input can never
// succeed, so the engine's per-node retry loop should stop immediately
// instead of burning the configured MaxTries budget (and the wall-clock
// backoff time that goes with it) on attempts that are guaranteed to
// fail identically. This is distinct from a transient error (429/5xx,
// network blip), which the engine retries as configured.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps err so the engine's retry loop treats it as a
// permanent/configuration error: it still fails the node (and the run,
// unless ContinueOnFail is set) with a clear message, but does not
// waste retry attempts or backoff time on it. Node executors call this
// for errors they know retrying can't fix -- e.g. an HTTP 401/403
// response, or a required credential/config value that's empty.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked
// via Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// fatalError marks a configuration error that must stop the run even when the
// failing node has ContinueOnFail set (e.g. GOOGLE_SHEETS_CONFIG_ERROR: reading
// or writing the wrong spreadsheet tab is worse than stopping). A fatal error is
// also permanent, so it is never retried.
type fatalError struct{ err error }

func (f *fatalError) Error() string { return f.err.Error() }
func (f *fatalError) Unwrap() error { return f.err }

// Fatal wraps err so the engine neither retries it nor turns it into a
// continue-on-fail error item: the node fails and the run stops.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return &fatalError{err: &permanentError{err: err}}
}

// IsFatal reports whether err (or anything it wraps) was marked via Fatal.
func IsFatal(err error) bool {
	var f *fatalError
	return errors.As(err, &f)
}
