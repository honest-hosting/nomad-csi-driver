//go:build tools

// Package tools pins toolchain dependencies so `go mod tidy` keeps them in
// go.sum. The build tag prevents this file from being compiled into binaries.
package tools
