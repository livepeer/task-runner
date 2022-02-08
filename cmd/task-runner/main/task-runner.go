package main

// This file intentionally minimal so that the analyzer binary
// can be imported and executed elsewhere. Main content of the
// CLI is in cmd/analyzer.go.

import (
	taskrunner "github.com/livepeer/task-runner/cmd/task-runner"
)

// Version content of this constant will be set at build time,
// using -ldflags, using output of the `git describe` command.
var Version = "undefined"

func main() {
	taskrunner.Run(taskrunner.BuildFlags{
		Version: Version,
	})
}
