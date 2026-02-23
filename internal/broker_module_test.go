package internal

import (
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestNewBrokerModule_EmptyConfig(t *testing.T) {
	m, err := newBrokerModule("test-broker", map[string]any{})
	if err != nil {
		t.Fatalf("newBrokerModule() returned error: %v", err)
	}
	if m == nil {
		t.Fatal("newBrokerModule() returned nil")
	}
}

func TestNewBrokerModule_WithTransport(t *testing.T) {
	config := map[string]any{
		"transport": "nats",
		"transport_config": map[string]any{
			"urls": []string{"nats://localhost:4222"},
		},
	}

	m, err := newBrokerModule("test-broker", config)
	if err != nil {
		t.Fatalf("newBrokerModule() returned error: %v", err)
	}
	if m == nil {
		t.Fatal("newBrokerModule() returned nil")
	}
}

func TestBrokerModule_Name(t *testing.T) {
	m, _ := newBrokerModule("my-broker", map[string]any{})
	if m.name != "my-broker" {
		t.Errorf("brokerModule.name = %q, want %q", m.name, "my-broker")
	}
}

func TestBrokerModule_Init_EmptyConfig_DefaultsToMemory(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})

	err := m.Init()
	if err != nil {
		t.Fatalf("Init() with empty config returned unexpected error: %v", err)
	}

	if m.transport != "memory" {
		t.Errorf("transport = %q, want %q", m.transport, "memory")
	}
}

func TestBrokerModule_Init_ExplicitTransport(t *testing.T) {
	config := map[string]any{
		"transport": "kafka",
	}

	m, _ := newBrokerModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.transport != "kafka" {
		t.Errorf("transport = %q, want %q", m.transport, "kafka")
	}
}

func TestBrokerModule_Init_EmptyTransportString_DefaultsToMemory(t *testing.T) {
	config := map[string]any{
		"transport": "",
	}

	m, _ := newBrokerModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.transport != "memory" {
		t.Errorf("transport = %q, want %q (should default to memory)", m.transport, "memory")
	}
}

func TestBrokerModule_Init_TransportConfig_IsStored(t *testing.T) {
	config := map[string]any{
		"transport": "nats",
		"transport_config": map[string]any{
			"urls": "nats://localhost:4222",
		},
	}

	m, _ := newBrokerModule("test", config)
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.transportConfig == nil {
		t.Fatal("transportConfig should not be nil after Init()")
	}
	if _, ok := m.transportConfig["urls"]; !ok {
		t.Error("transportConfig missing 'urls' key")
	}
}

func TestBrokerModule_Init_NoTransportConfig_DefaultsToEmptyMap(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})
	if err := m.Init(); err != nil {
		t.Fatalf("Init() returned unexpected error: %v", err)
	}

	if m.transportConfig == nil {
		t.Error("transportConfig should not be nil when not provided")
	}
}

func TestBrokerModule_SetMessagePublisher(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})

	pub := &mockPublisher{}
	m.SetMessagePublisher(pub)

	if m.publisher == nil {
		t.Error("SetMessagePublisher() did not set publisher")
	}
}

func TestBrokerModule_SetMessageSubscriber(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})

	sub := &mockSubscriber{}
	m.SetMessageSubscriber(sub)

	if m.subscriber == nil {
		t.Error("SetMessageSubscriber() did not set subscriber")
	}
}

func TestBrokerModule_StreamsMap_IsInitialized(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})
	if m.streams == nil {
		t.Error("brokerModule.streams map should be initialized")
	}
}

func TestBrokerModule_ImplementsMessageAwareModule(t *testing.T) {
	// Compile-time check: brokerModule must implement MessageAwareModule.
	// We call a method to exercise the interface at runtime.
	m, _ := newBrokerModule("test", map[string]any{})
	var _ sdk.MessageAwareModule = m
	m.SetMessagePublisher(nil)
	m.SetMessageSubscriber(nil)
}

func TestBrokerModule_Start_IsNoOp(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})
	m.Init() //nolint:errcheck

	// Start should not return an error for a broker (streams created on demand).
	if err := m.Start(nil); err != nil { //nolint:staticcheck
		t.Errorf("Start() returned unexpected error: %v", err)
	}
}
