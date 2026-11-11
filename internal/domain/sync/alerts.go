package sync

// Incident is a sanitized operational failure. Empty severity denotes recovery.
// Callers must use fixed descriptions and numeric codes, never upstream messages.
type Incident struct {
	Severity  string `json:"severity,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Code      string `json:"code,omitempty"`
	Suspended bool   `json:"suspended,omitempty"`
}

// AlertReporter queues incident transitions without performing network I/O.
type AlertReporter interface {
	Report(Incident)
}

// SyncObserver receives the outcome after snapshot and required activity persistence.
type SyncObserver interface {
	SyncFinished(error)
}

// SyncErrorSanitizer supplies safe operational errors for application logging.
type SyncErrorSanitizer interface {
	SafeSyncError(error) error
}
