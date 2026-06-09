// Package version holds build-stamp variables populated at link time via
// -ldflags (see the Makefile). They are intentionally plain vars so the linker
// can overwrite them; nothing else should mutate them at runtime.
package version

// Build metadata, set via -ldflags "-X .../version.X=...". The defaults apply
// to `go run`/`go test` builds that do not pass ldflags.
var (
	// Version is the semantic version or "vLOCALDEV" for un-stamped builds.
	Version = "vLOCALDEV"
	// CommitSHA is the git commit the binary was built from.
	CommitSHA = "unknown"
	// BuildDate is an ISO-8601 timestamp of the build.
	BuildDate = "unknown"
)

// String returns a single-line human-readable build identifier.
func String() string {
	return Version + " (" + CommitSHA + ", built " + BuildDate + ")"
}
