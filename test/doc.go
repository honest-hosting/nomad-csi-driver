// Package e2e contains the full Nomad end-to-end test suite, which drives a
// real nomad agent with the plugin registered. The tests are build-tag gated
// (//go:build integration) and live in e2e_test.go; this file exists so the
// package always has a non-test Go file (keeping `go build ./...` happy when
// the integration tag is absent).
package e2e
