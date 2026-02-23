package internal

import (
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestNewBentoPlugin(t *testing.T) {
	plugin := NewBentoPlugin()
	if plugin == nil {
		t.Fatal("NewBentoPlugin() returned nil")
	}

	// Verify it implements PluginProvider
	var _ sdk.PluginProvider = plugin
}

func TestBentoPlugin_Manifest(t *testing.T) {
	plugin := NewBentoPlugin()
	manifest := plugin.Manifest()

	if manifest.Name != "bento" {
		t.Errorf("expected name 'bento', got %q", manifest.Name)
	}
	if manifest.Version != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", manifest.Version)
	}
	if manifest.Author != "GoCodeAlone" {
		t.Errorf("expected author 'GoCodeAlone', got %q", manifest.Author)
	}
	if manifest.Description == "" {
		t.Error("expected non-empty description")
	}
}

func TestBentoPlugin_ModuleTypes(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to ModuleProvider
	moduleProvider, ok := plugin.(sdk.ModuleProvider)
	if !ok {
		t.Fatal("plugin does not implement ModuleProvider")
	}

	types := moduleProvider.ModuleTypes()

	expected := []string{"bento.stream", "bento.input", "bento.output", "bento.broker"}
	if len(types) != len(expected) {
		t.Errorf("expected %d module types, got %d", len(expected), len(types))
	}

	typeSet := make(map[string]bool)
	for _, typ := range types {
		typeSet[typ] = true
	}

	for _, exp := range expected {
		if !typeSet[exp] {
			t.Errorf("expected module type %q not found in %v", exp, types)
		}
	}
}

func TestBentoPlugin_CreateModule(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to ModuleProvider
	moduleProvider, ok := plugin.(sdk.ModuleProvider)
	if !ok {
		t.Fatal("plugin does not implement ModuleProvider")
	}

	tests := []struct {
		typeName string
		name     string
		config   map[string]any
		wantErr  bool
	}{
		{
			typeName: "bento.stream",
			name:     "test-stream",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			typeName: "bento.input",
			name:     "test-input",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			typeName: "bento.output",
			name:     "test-output",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			typeName: "bento.broker",
			name:     "test-broker",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			typeName: "unknown.type",
			name:     "test",
			config:   map[string]any{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			module, err := moduleProvider.CreateModule(tt.typeName, tt.name, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateModule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if module == nil {
					t.Error("expected non-nil module")
				}
				// Verify it implements ModuleInstance
				var _ sdk.ModuleInstance = module
			}
		})
	}
}

func TestBentoPlugin_StepTypes(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to StepProvider
	stepProvider, ok := plugin.(sdk.StepProvider)
	if !ok {
		t.Fatal("plugin does not implement StepProvider")
	}

	types := stepProvider.StepTypes()

	expected := []string{"step.bento"}
	if len(types) != len(expected) {
		t.Errorf("expected %d step types, got %d", len(expected), len(types))
	}

	if types[0] != expected[0] {
		t.Errorf("expected step type %q, got %q", expected[0], types[0])
	}
}

func TestBentoPlugin_CreateStep(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to StepProvider
	stepProvider, ok := plugin.(sdk.StepProvider)
	if !ok {
		t.Fatal("plugin does not implement StepProvider")
	}

	tests := []struct {
		typeName string
		name     string
		config   map[string]any
		wantErr  bool
	}{
		{
			typeName: "step.bento",
			name:     "test-step",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			typeName: "unknown.step",
			name:     "test",
			config:   map[string]any{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			step, err := stepProvider.CreateStep(tt.typeName, tt.name, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateStep() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if step == nil {
					t.Error("expected non-nil step")
				}
				// Verify it implements StepInstance
				var _ sdk.StepInstance = step
			}
		})
	}
}

func TestBentoPlugin_TriggerTypes(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to TriggerProvider
	triggerProvider, ok := plugin.(sdk.TriggerProvider)
	if !ok {
		t.Fatal("plugin does not implement TriggerProvider")
	}

	types := triggerProvider.TriggerTypes()

	expected := []string{"bento"}
	if len(types) != len(expected) {
		t.Errorf("expected %d trigger types, got %d", len(expected), len(types))
	}

	if types[0] != expected[0] {
		t.Errorf("expected trigger type %q, got %q", expected[0], types[0])
	}
}

func TestBentoPlugin_CreateTrigger(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to TriggerProvider
	triggerProvider, ok := plugin.(sdk.TriggerProvider)
	if !ok {
		t.Fatal("plugin does not implement TriggerProvider")
	}

	mockCallback := func(action string, data map[string]any) error {
		return nil
	}

	tests := []struct {
		typeName string
		config   map[string]any
		wantErr  bool
	}{
		{
			typeName: "bento",
			config:   map[string]any{},
			wantErr:  false,
		},
		{
			typeName: "unknown.trigger",
			config:   map[string]any{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			trigger, err := triggerProvider.CreateTrigger(tt.typeName, tt.config, mockCallback)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTrigger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if trigger == nil {
					t.Error("expected non-nil trigger")
				}
				// Verify it implements TriggerInstance
				var _ sdk.TriggerInstance = trigger
			}
		})
	}
}

func TestBentoPlugin_ModuleSchemas(t *testing.T) {
	plugin := NewBentoPlugin()

	// Cast to SchemaProvider
	schemaProvider, ok := plugin.(sdk.SchemaProvider)
	if !ok {
		t.Fatal("plugin does not implement SchemaProvider")
	}

	schemas := schemaProvider.ModuleSchemas()

	if len(schemas) == 0 {
		t.Fatal("expected non-empty schemas")
	}

	// Build a map for easy lookup
	schemaMap := make(map[string]sdk.ModuleSchemaData)
	for _, schema := range schemas {
		schemaMap[schema.Type] = schema
	}

	// Verify all module types have schemas
	expectedTypes := []string{"bento.stream", "bento.input", "bento.output", "bento.broker", "step.bento"}
	for _, typeName := range expectedTypes {
		schema, found := schemaMap[typeName]
		if !found {
			t.Errorf("missing schema for type %q", typeName)
			continue
		}

		if schema.Label == "" {
			t.Errorf("schema %q: empty label", typeName)
		}
		if schema.Category == "" {
			t.Errorf("schema %q: empty category", typeName)
		}
		if schema.Description == "" {
			t.Errorf("schema %q: empty description", typeName)
		}
	}

	// Verify specific schema details
	t.Run("bento.stream schema", func(t *testing.T) {
		schema := schemaMap["bento.stream"]
		if len(schema.ConfigFields) == 0 {
			t.Error("expected config fields for bento.stream")
		}
	})

	t.Run("bento.input schema", func(t *testing.T) {
		schema := schemaMap["bento.input"]
		if len(schema.ConfigFields) == 0 {
			t.Error("expected config fields for bento.input")
		}
		if len(schema.Outputs) == 0 {
			t.Error("expected outputs for bento.input")
		}

		// Check for required fields
		hasTargetTopic := false
		for _, field := range schema.ConfigFields {
			if field.Name == "target_topic" && field.Required {
				hasTargetTopic = true
			}
		}
		if !hasTargetTopic {
			t.Error("expected required target_topic field in bento.input schema")
		}
	})

	t.Run("bento.output schema", func(t *testing.T) {
		schema := schemaMap["bento.output"]
		if len(schema.ConfigFields) == 0 {
			t.Error("expected config fields for bento.output")
		}
		if len(schema.Inputs) == 0 {
			t.Error("expected inputs for bento.output")
		}

		// Check for required fields
		hasSourceTopic := false
		for _, field := range schema.ConfigFields {
			if field.Name == "source_topic" && field.Required {
				hasSourceTopic = true
			}
		}
		if !hasSourceTopic {
			t.Error("expected required source_topic field in bento.output schema")
		}
	})

	t.Run("step.bento schema", func(t *testing.T) {
		schema := schemaMap["step.bento"]
		if len(schema.ConfigFields) == 0 {
			t.Error("expected config fields for step.bento")
		}

		// Check for processors field
		hasProcessors := false
		for _, field := range schema.ConfigFields {
			if field.Name == "processors" {
				hasProcessors = true
			}
		}
		if !hasProcessors {
			t.Error("expected processors field in step.bento schema")
		}
	})
}

func TestBentoPlugin_Interfaces(t *testing.T) {
	plugin := NewBentoPlugin()

	// Verify all expected interfaces are implemented
	var _ sdk.PluginProvider = plugin

	if _, ok := plugin.(sdk.ModuleProvider); !ok {
		t.Error("plugin does not implement ModuleProvider")
	}
	if _, ok := plugin.(sdk.StepProvider); !ok {
		t.Error("plugin does not implement StepProvider")
	}
	if _, ok := plugin.(sdk.TriggerProvider); !ok {
		t.Error("plugin does not implement TriggerProvider")
	}
	if _, ok := plugin.(sdk.SchemaProvider); !ok {
		t.Error("plugin does not implement SchemaProvider")
	}
}
