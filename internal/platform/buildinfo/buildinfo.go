// Package buildinfo reports which build of ASQL is running.
package buildinfo

import (
	"runtime/debug"
	"sync"
)

var engineVersion = sync.OnceValue(func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	revision, modified := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if revision == "" {
		return "devel"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		revision += "-dirty"
	}
	return "devel+" + revision
})

// EngineVersion is the module version of the running binary, or
// devel+<commit> for a build from a source checkout. Results attested by
// different versions are not guaranteed to be equal (ADR 0006).
func EngineVersion() string { return engineVersion() }
