package clock

import "time"

// Realtime provides wall-clock time for production runtime paths.
type Realtime struct{}

// Now returns current wall-clock time.
func (Realtime) Now() time.Time {
	return time.Now().UTC()
}
