package main

//go:generate go generate ../../internal/entry

import "github.com/Wei-Shaw/sub2api/internal/entry"

// Keep the main.* linker symbols used by the existing release builds.
var (
	Version   = ""
	Commit    = "unknown"
	Date      = "unknown"
	BuildType = "source"
)

func main() {
	entry.Run(entry.BuildInfo{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		BuildType: BuildType,
	})
}
