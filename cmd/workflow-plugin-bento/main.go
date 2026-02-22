// Command workflow-plugin-bento is a workflow engine external plugin that
// integrates Bento stream processing. It runs as a subprocess and communicates
// with the host workflow engine via the go-plugin protocol.
package main

import (
	"github.com/GoCodeAlone/workflow-plugin-bento/internal"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func main() {
	sdk.Serve(internal.NewBentoPlugin())
}
