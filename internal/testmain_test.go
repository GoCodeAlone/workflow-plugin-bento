package internal

import (
	// Register pure Bento components (e.g. generate input, bloblang processor)
	// so that tests can use them without importing the full component suite.
	_ "github.com/warpstreamlabs/bento/v4/public/components/pure"
)
