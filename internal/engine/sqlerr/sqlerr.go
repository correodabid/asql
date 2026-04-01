package sqlerr

// SQLError is an error that carries a PostgreSQL SQLSTATE code.
// Sentinel errors created with New can be wrapped via fmt.Errorf("%w: …", sentinel)
// and the SQLSTATE code will be recoverable through errors.As.
type SQLError struct {
	Code    string // 5-char SQLSTATE, e.g. "42P01"
	Message string
}

func (e *SQLError) Error() string { return e.Message }

// New creates a *SQLError suitable for use as a sentinel error.
func New(code, message string) *SQLError {
	return &SQLError{Code: code, Message: message}
}
