// Package internal implements the bento workflow plugin.
package internal

import (
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// bentoPlugin implements PluginProvider, ModuleProvider, StepProvider, TriggerProvider.
type bentoPlugin struct{}

// NewBentoPlugin returns a new bentoPlugin instance.
func NewBentoPlugin() sdk.PluginProvider {
	return &bentoPlugin{}
}

// Manifest returns plugin metadata.
func (p *bentoPlugin) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "bento",
		Version:     "1.0.0",
		Author:      "GoCodeAlone",
		Description: "Stream processing via Bento — 100+ connectors, Bloblang transforms, at-least-once delivery",
	}
}

// ModuleTypes returns the module type names this plugin provides.
func (p *bentoPlugin) ModuleTypes() []string {
	return []string{"bento.stream", "bento.input", "bento.output", "bento.broker"}
}

// CreateModule creates a module instance of the given type.
func (p *bentoPlugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "bento.stream":
		return newStreamModule(name, config)
	case "bento.input":
		return newInputModule(name, config)
	case "bento.output":
		return newOutputModule(name, config)
	case "bento.broker":
		return newBrokerModule(name, config)
	default:
		return nil, fmt.Errorf("unknown module type: %s", typeName)
	}
}

// StepTypes returns the step type names this plugin provides.
func (p *bentoPlugin) StepTypes() []string {
	return []string{"step.bento"}
}

// CreateStep creates a step instance of the given type.
func (p *bentoPlugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.bento":
		return newProcessorStep(name, config)
	default:
		return nil, fmt.Errorf("unknown step type: %s", typeName)
	}
}

// TriggerTypes returns the trigger type names this plugin provides.
// Note: trigger support via the external plugin gRPC bridge is not yet fully
// implemented in the host adapter (CreateTrigger RPC is missing). Returning an
// empty list avoids a registration failure that would prevent step.bento from
// loading. Triggers can be re-enabled once the host adapter supports them.
func (p *bentoPlugin) TriggerTypes() []string {
	return []string{}
}

// CreateTrigger creates a trigger instance of the given type.
func (p *bentoPlugin) CreateTrigger(typeName string, config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error) {
	switch typeName {
	case "bento":
		return newBentoTrigger(config, cb)
	default:
		return nil, fmt.Errorf("unknown trigger type: %s", typeName)
	}
}

// ModuleSchemas returns UI schemas for all module types.
func (p *bentoPlugin) ModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "bento.stream",
			Label:       "Bento Stream",
			Category:    "streaming",
			Description: "Full Bento stream: input → processors → output. Supports 100+ connectors.",
			ConfigFields: []sdk.ConfigField{
				{Name: "input", Type: "object", Description: "Bento input configuration", Required: true},
				{Name: "pipeline", Type: "object", Description: "Bento pipeline (processors) configuration"},
				{Name: "output", Type: "object", Description: "Bento output configuration", Required: true},
				{Name: "logger", Type: "object", Description: "Bento logger configuration"},
				{Name: "metrics", Type: "object", Description: "Bento metrics configuration"},
			},
		},
		{
			Type:        "bento.input",
			Label:       "Bento Input",
			Category:    "streaming",
			Description: "Bento input as event source. Publishes received messages to the host EventBus.",
			ConfigFields: []sdk.ConfigField{
				{Name: "input", Type: "object", Description: "Bento input configuration", Required: true},
				{Name: "target_topic", Type: "string", Description: "Host EventBus topic to publish messages to", Required: true},
				{Name: "target_broker", Type: "string", Description: "Optional broker name on the host"},
			},
			Outputs: []sdk.ServiceIO{
				{Name: "messages", Type: "EventBus", Description: "Messages published to the host EventBus"},
			},
		},
		{
			Type:        "bento.output",
			Label:       "Bento Output",
			Category:    "streaming",
			Description: "Bento output as event sink. Subscribes to host EventBus topic and forwards to Bento.",
			ConfigFields: []sdk.ConfigField{
				{Name: "output", Type: "object", Description: "Bento output configuration", Required: true},
				{Name: "source_topic", Type: "string", Description: "Host EventBus topic to subscribe to", Required: true},
				{Name: "source_broker", Type: "string", Description: "Optional broker name on the host"},
			},
			Inputs: []sdk.ServiceIO{
				{Name: "messages", Type: "EventBus", Description: "Messages received from the host EventBus"},
			},
		},
		{
			Type:        "bento.broker",
			Label:       "Bento Broker",
			Category:    "messaging",
			Description: "Drop-in MessageBroker implemented via Bento transport. Creates per-topic streams on demand.",
			ConfigFields: []sdk.ConfigField{
				{Name: "transport", Type: "string", Description: "Bento transport type (e.g. kafka, nats, memory)", DefaultValue: "memory"},
				{Name: "transport_config", Type: "object", Description: "Transport-specific configuration"},
			},
		},
		{
			Type:        "step.bento",
			Label:       "Bento Processor Step",
			Category:    "transform",
			Description: "Runs data through Bento processors (Bloblang, jmespath, etc.) as a pipeline step.",
			ConfigFields: []sdk.ConfigField{
				{Name: "processors", Type: "string", Description: "YAML string or map defining Bento processors", Required: true},
			},
		},
	}
}
