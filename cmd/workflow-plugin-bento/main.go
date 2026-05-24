// Command workflow-plugin-bento is a workflow engine external plugin that
// integrates Bento stream processing. It runs as a subprocess and communicates
// with the host workflow engine via the go-plugin protocol.
package main

import (
	"github.com/GoCodeAlone/workflow-plugin-bento/internal"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"

	// Register pure Bento components (mapping/bloblang processors, generate input, etc.)
	// so that step.bento can execute Bloblang transforms at runtime.
	_ "github.com/warpstreamlabs/bento/v4/public/components/pure"
)

func main() {
	sdk.Serve(internal.NewBentoPlugin(), sdk.WithBuildVersion(sdk.ResolveBuildVersion(internal.Version)))
}
