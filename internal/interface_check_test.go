package internal

import (
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Compile-time interface checks.
var (
	_ sdk.PluginProvider  = (*bentoPlugin)(nil)
	_ sdk.ModuleProvider  = (*bentoPlugin)(nil)
	_ sdk.StepProvider    = (*bentoPlugin)(nil)
	_ sdk.TriggerProvider = (*bentoPlugin)(nil)
	_ sdk.SchemaProvider  = (*bentoPlugin)(nil)

	_ sdk.ModuleInstance     = (*streamModule)(nil)
	_ sdk.ModuleInstance     = (*inputModule)(nil)
	_ sdk.ModuleInstance     = (*outputModule)(nil)
	_ sdk.ModuleInstance     = (*brokerModule)(nil)
	_ sdk.MessageAwareModule = (*inputModule)(nil)
	_ sdk.MessageAwareModule = (*outputModule)(nil)
	_ sdk.MessageAwareModule = (*brokerModule)(nil)

	_ sdk.StepInstance    = (*processorStep)(nil)
	_ sdk.TriggerInstance = (*bentoTrigger)(nil)
)
