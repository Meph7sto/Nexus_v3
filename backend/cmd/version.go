package cmd

import (
	_ "embed"
	"strings"
)

// The release tooling writes this file, so both entrypoints embed it in place.
//
//go:embed server/VERSION
var embeddedVersion string

// ResolveVersion prefers the linker-injected version over the embedded one.
func ResolveVersion(version string) string {
	if strings.TrimSpace(version) != "" {
		return version
	}
	if version = strings.TrimSpace(embeddedVersion); version != "" {
		return version
	}
	return "0.0.0-dev"
}
