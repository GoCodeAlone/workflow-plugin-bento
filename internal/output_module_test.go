package internal

import (
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestNewOutputModule_ValidConfig(t *testing.T) {
	config := map[string]any{
		"source_topic": "events",
		"output":       map[string]any{"drop": map[string]any{}},
	}

	m, err := newOutputModule("test-output", config)
	if err != nil {
		t.Fatalf("newOutputModule() returned error: %v", err)
	}
	if m == nil {
		t.Fatal("newOutputModule() returned nil")
	}
}

func TestNewOutputModule_EmptyConfig(t *testing.T) {
	// Constructor succeeds; Init validates required fields.
	m, err := newOutputModule("test-output", map[string]any{})
	if err != nil {
		t.Fatalf("newOutputModule() with empty config returned unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("newOutputModule() returned nil")
	}
}

func TestOutputModule_Name(t *testing.T) {
	m, _ := newOutputModule("my-output", map[string]any{})
	if m.name != "my-output" {
		t.Errorf("outputModule.name = %q, want %q", m.name, "my-output")
	}
}

func TestOutputModule_Init_MissingSourceTopic_ReturnsError(t *testing.T) {
	config := map[string]any{
		"output": map[string]any{"drop": map[string]any{}},
	}

	m, _ := newOutputModule("test", config)
	err := m.Init()
	if err == nil {
		t.Fatal("Init() without source_topic should return error")
	}
}

func TestOutputModule_Init_EmptySourceTopic_ReturnsError(t *testing.T) {
	config := map[string]any{
		"source_topic": "",
		"output":       map[string]any{"drop": map[string]any{}},
	}

	m, _ := newOutputModule("test", config)
	err := m.Init()
	if err == nil {
		t.Fatal("Init() with empty source_topic should return error")
	}
}

func TestOutputModule_Init_MissingOutputConfig_ReturnsError(t *testing.T) {
	config := map[string]any{
		"source_topic": "events",
		// "output" key is intentionally missing
	}

	m, _ := newOutputModule("test", config)
	err := m.Init()
	if err == nil {
		t.Fatal("Init() without output configuration should return error")
	}
}

func TestOutputModule_Init_ValidConfig_SetsSourceTopic(t *testing.T) {
	config := map[string]any{
		"source_topic": "my-events",
		"output":       map[string]any{"drop": map[string]any{}},
	}

	m, _ := newOutputModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.sourceTopic != "my-events" {
		t.Errorf("sourceTopic = %q, want %q", m.sourceTopic, "my-events")
	}
}

func TestOutputModule_Init_WithSourceBroker_SetsField(t *testing.T) {
	config := map[string]any{
		"source_topic":  "my-events",
		"source_broker": "my-broker",
		"output":        map[string]any{"drop": map[string]any{}},
	}

	m, _ := newOutputModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.sourceBroker != "my-broker" {
		t.Errorf("sourceBroker = %q, want %q", m.sourceBroker, "my-broker")
	}
}

func TestOutputModule_Init_WithoutSourceBroker_DefaultsToEmpty(t *testing.T) {
	config := map[string]any{
		"source_topic": "my-events",
		"output":       map[string]any{"drop": map[string]any{}},
	}

	m, _ := newOutputModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.sourceBroker != "" {
		t.Errorf("sourceBroker = %q, want empty string", m.sourceBroker)
	}
}

func TestOutputModule_SetMessageSubscriber(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{})

	sub := &mockSubscriber{}
	m.SetMessageSubscriber(sub)

	if m.subscriber == nil {
		t.Error("SetMessageSubscriber() did not set subscriber")
	}
}

func TestOutputModule_SetMessagePublisher_IsNoOp(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{})

	// Should not panic; publisher is ignored by outputModule.
	pub := &mockPublisher{}
	m.SetMessagePublisher(pub)
}

func TestOutputModule_DoneChannel_IsInitialized(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{})
	if m.done == nil {
		t.Error("outputModule.done channel should be initialized")
	}
}

func TestOutputModule_ImplementsMessageAwareModule(t *testing.T) {
	// Compile-time check: outputModule must implement MessageAwareModule.
	// We call a method to exercise the interface at runtime.
	m, _ := newOutputModule("test", map[string]any{})
	var _ sdk.MessageAwareModule = m
	m.SetMessagePublisher(nil)
	m.SetMessageSubscriber(nil)
}
