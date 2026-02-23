package internal

import (
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestNewInputModule_ValidConfig(t *testing.T) {
	config := map[string]any{
		"target_topic": "events",
		"input":        map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
	}

	m, err := newInputModule("test-input", config)
	if err != nil {
		t.Fatalf("newInputModule() returned error: %v", err)
	}
	if m == nil {
		t.Fatal("newInputModule() returned nil")
	}
}

func TestNewInputModule_EmptyConfig(t *testing.T) {
	// Constructor succeeds; Init validates required fields.
	m, err := newInputModule("test-input", map[string]any{})
	if err != nil {
		t.Fatalf("newInputModule() with empty config returned unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("newInputModule() returned nil")
	}
}

func TestInputModule_Name(t *testing.T) {
	m, _ := newInputModule("my-input", map[string]any{})
	if m.name != "my-input" {
		t.Errorf("inputModule.name = %q, want %q", m.name, "my-input")
	}
}

func TestInputModule_Init_MissingTargetTopic_ReturnsError(t *testing.T) {
	config := map[string]any{
		"input": map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
	}

	m, _ := newInputModule("test", config)
	err := m.Init()
	if err == nil {
		t.Fatal("Init() without target_topic should return error")
	}
}

func TestInputModule_Init_EmptyTargetTopic_ReturnsError(t *testing.T) {
	config := map[string]any{
		"target_topic": "",
		"input":        map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
	}

	m, _ := newInputModule("test", config)
	err := m.Init()
	if err == nil {
		t.Fatal("Init() with empty target_topic should return error")
	}
}

func TestInputModule_Init_MissingInputConfig_ReturnsError(t *testing.T) {
	config := map[string]any{
		"target_topic": "events",
		// "input" key is intentionally missing
	}

	m, _ := newInputModule("test", config)
	err := m.Init()
	if err == nil {
		t.Fatal("Init() without input configuration should return error")
	}
}

func TestInputModule_Init_ValidConfig_SetsTargetTopic(t *testing.T) {
	config := map[string]any{
		"target_topic": "my-events",
		"input":        map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
	}

	m, _ := newInputModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.targetTopic != "my-events" {
		t.Errorf("targetTopic = %q, want %q", m.targetTopic, "my-events")
	}
}

func TestInputModule_Init_WithTargetBroker_SetsField(t *testing.T) {
	config := map[string]any{
		"target_topic":  "my-events",
		"target_broker": "my-broker",
		"input":         map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
	}

	m, _ := newInputModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.targetBroker != "my-broker" {
		t.Errorf("targetBroker = %q, want %q", m.targetBroker, "my-broker")
	}
}

func TestInputModule_Init_WithoutTargetBroker_DefaultsToEmpty(t *testing.T) {
	config := map[string]any{
		"target_topic": "my-events",
		"input":        map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
	}

	m, _ := newInputModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.targetBroker != "" {
		t.Errorf("targetBroker = %q, want empty string", m.targetBroker)
	}
}

func TestInputModule_SetMessagePublisher(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{})

	pub := &mockPublisher{}
	m.SetMessagePublisher(pub)

	if m.publisher == nil {
		t.Error("SetMessagePublisher() did not set publisher")
	}
}

func TestInputModule_SetMessageSubscriber_IsNoOp(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{})

	// Should not panic and subscriber should remain nil.
	sub := &mockSubscriber{}
	m.SetMessageSubscriber(sub)
	// inputModule ignores the subscriber
}

func TestInputModule_DoneChannel_IsInitialized(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{})
	if m.done == nil {
		t.Error("inputModule.done channel should be initialized")
	}
}

func TestInputModule_ImplementsMessageAwareModule(t *testing.T) {
	// Compile-time check: inputModule must implement MessageAwareModule.
	// We call a method to exercise the interface at runtime.
	m, _ := newInputModule("test", map[string]any{})
	var _ sdk.MessageAwareModule = m
	m.SetMessagePublisher(nil)
	m.SetMessageSubscriber(nil)
}
