package internal

import (
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestNewBentoPlugin_ReturnsPluginProvider(t *testing.T) {
	p := NewBentoPlugin()
	if p == nil {
		t.Fatal("NewBentoPlugin() returned nil")
	}
}

func TestBentoPlugin_Manifest(t *testing.T) {
	p := NewBentoPlugin()
	m := p.Manifest()

	if m.Name != "bento" {
		t.Errorf("Manifest().Name = %q, want %q", m.Name, "bento")
	}
	if m.Version == "" {
		t.Error("Manifest().Version is empty")
	}
	if m.Author == "" {
		t.Error("Manifest().Author is empty")
	}
	if m.Description == "" {
		t.Error("Manifest().Description is empty")
	}
}

func TestBentoPlugin_ModuleTypes(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)
	types := p.ModuleTypes()

	want := map[string]bool{
		"bento.stream": false,
		"bento.input":  false,
		"bento.output": false,
		"bento.broker": false,
	}

	for _, typ := range types {
		if _, ok := want[typ]; ok {
			want[typ] = true
		} else {
			t.Errorf("unexpected module type %q", typ)
		}
	}

	for typ, seen := range want {
		if !seen {
			t.Errorf("module type %q not returned by ModuleTypes()", typ)
		}
	}
}

func TestBentoPlugin_StepTypes(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)
	types := p.StepTypes()

	if len(types) != 1 {
		t.Fatalf("StepTypes() returned %d types, want 1", len(types))
	}
	if types[0] != "step.bento" {
		t.Errorf("StepTypes()[0] = %q, want %q", types[0], "step.bento")
	}
}

func TestBentoPlugin_TriggerTypes(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)
	types := p.TriggerTypes()

	if len(types) != 1 {
		t.Fatalf("TriggerTypes() returned %d types, want 1", len(types))
	}
	if types[0] != "bento" {
		t.Errorf("TriggerTypes()[0] = %q, want %q", types[0], "bento")
	}
}

func TestBentoPlugin_CreateModule_ValidTypes(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)

	tests := []struct {
		typeName string
		config   map[string]any
	}{
		{
			typeName: "bento.stream",
			config: map[string]any{
				"input":  map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
				"output": map[string]any{"drop": map[string]any{}},
			},
		},
		{
			typeName: "bento.input",
			config: map[string]any{
				"target_topic": "test-topic",
				"input":        map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
			},
		},
		{
			typeName: "bento.output",
			config: map[string]any{
				"source_topic": "test-topic",
				"output":       map[string]any{"drop": map[string]any{}},
			},
		},
		{
			typeName: "bento.broker",
			config:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			mod, err := p.CreateModule(tt.typeName, "test-"+tt.typeName, tt.config)
			if err != nil {
				t.Fatalf("CreateModule(%q) returned error: %v", tt.typeName, err)
			}
			if mod == nil {
				t.Fatalf("CreateModule(%q) returned nil module", tt.typeName)
			}
		})
	}
}

func TestBentoPlugin_CreateModule_UnknownType(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)

	mod, err := p.CreateModule("bento.unknown", "test", map[string]any{})
	if err == nil {
		t.Fatal("CreateModule with unknown type should return error")
	}
	if mod != nil {
		t.Error("CreateModule with unknown type should return nil module")
	}
}

func TestBentoPlugin_CreateStep_ValidType(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)

	step, err := p.CreateStep("step.bento", "test-step", map[string]any{})
	if err != nil {
		t.Fatalf("CreateStep(step.bento) returned error: %v", err)
	}
	if step == nil {
		t.Fatal("CreateStep(step.bento) returned nil step")
	}
}

func TestBentoPlugin_CreateStep_UnknownType(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)

	step, err := p.CreateStep("step.unknown", "test", map[string]any{})
	if err == nil {
		t.Fatal("CreateStep with unknown type should return error")
	}
	if step != nil {
		t.Error("CreateStep with unknown type should return nil step")
	}
}

func TestBentoPlugin_CreateTrigger_ValidType(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)

	cb := func(action string, data map[string]any) error { return nil }
	trigger, err := p.CreateTrigger("bento", map[string]any{}, cb)
	if err != nil {
		t.Fatalf("CreateTrigger(bento) returned error: %v", err)
	}
	if trigger == nil {
		t.Fatal("CreateTrigger(bento) returned nil trigger")
	}
}

func TestBentoPlugin_CreateTrigger_UnknownType(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)

	cb := func(action string, data map[string]any) error { return nil }
	trigger, err := p.CreateTrigger("unknown", map[string]any{}, cb)
	if err == nil {
		t.Fatal("CreateTrigger with unknown type should return error")
	}
	if trigger != nil {
		t.Error("CreateTrigger with unknown type should return nil trigger")
	}
}

func TestBentoPlugin_ModuleSchemas(t *testing.T) {
	p := NewBentoPlugin().(*bentoPlugin)
	schemas := p.ModuleSchemas()

	if len(schemas) == 0 {
		t.Fatal("ModuleSchemas() returned empty slice")
	}

	schemaTypes := make(map[string]sdk.ModuleSchemaData)
	for _, s := range schemas {
		schemaTypes[s.Type] = s
	}

	expectedTypes := []string{"bento.stream", "bento.input", "bento.output", "bento.broker", "step.bento"}
	for _, typ := range expectedTypes {
		schema, ok := schemaTypes[typ]
		if !ok {
			t.Errorf("ModuleSchemas() missing schema for type %q", typ)
			continue
		}
		if schema.Label == "" {
			t.Errorf("schema for %q has empty Label", typ)
		}
		if schema.Description == "" {
			t.Errorf("schema for %q has empty Description", typ)
		}
	}
}

func TestBentoPlugin_ImplementsInterfaces(t *testing.T) {
	p := NewBentoPlugin()

	// Verify all required interfaces are satisfied at runtime.
	if _, ok := p.(sdk.ModuleProvider); !ok {
		t.Error("bentoPlugin does not implement sdk.ModuleProvider")
	}
	if _, ok := p.(sdk.StepProvider); !ok {
		t.Error("bentoPlugin does not implement sdk.StepProvider")
	}
	if _, ok := p.(sdk.TriggerProvider); !ok {
		t.Error("bentoPlugin does not implement sdk.TriggerProvider")
	}
	if _, ok := p.(sdk.SchemaProvider); !ok {
		t.Error("bentoPlugin does not implement sdk.SchemaProvider")
	}
}
