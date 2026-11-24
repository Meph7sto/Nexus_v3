package main

import "github.com/Wei-Shaw/sub2api/internal/entry"

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
