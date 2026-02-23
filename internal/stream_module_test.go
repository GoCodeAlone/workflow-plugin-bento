package internal

import (
	"context"
	"testing"
	"time"
)

func TestNewStreamModule_ValidConfig(t *testing.T) {
	config := map[string]any{
		"input":  map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
		"output": map[string]any{"drop": map[string]any{}},
	}

	m, err := newStreamModule("test-stream", config)
	if err != nil {
		t.Fatalf("newStreamModule() returned error: %v", err)
	}
	if m == nil {
		t.Fatal("newStreamModule() returned nil")
	}
}

func TestNewStreamModule_EmptyConfig(t *testing.T) {
	// Constructor accepts empty config; Init() validates it.
	m, err := newStreamModule("test-stream", map[string]any{})
	if err != nil {
		t.Fatalf("newStreamModule() with empty config returned unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("newStreamModule() returned nil")
	}
}

func TestStreamModule_Name(t *testing.T) {
	m, err := newStreamModule("my-stream", map[string]any{"key": "val"})
	if err != nil {
		t.Fatalf("newStreamModule() error: %v", err)
	}
	if m.name != "my-stream" {
		t.Errorf("streamModule.name = %q, want %q", m.name, "my-stream")
	}
}

func TestStreamModule_Init_EmptyConfig_ReturnsError(t *testing.T) {
	m, _ := newStreamModule("test", map[string]any{})
	err := m.Init()
	if err == nil {
		t.Fatal("Init() with empty config should return error")
	}
}

func TestStreamModule_Init_WithConfig_NoError(t *testing.T) {
	config := map[string]any{
		"input":  map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
		"output": map[string]any{"drop": map[string]any{}},
	}

	m, _ := newStreamModule("test", config)
	err := m.Init()
	if err != nil {
		t.Errorf("Init() with valid config returned error: %v", err)
	}
}

func TestStreamModule_Stop_WithoutStart(t *testing.T) {
	m, _ := newStreamModule("test", map[string]any{"key": "val"})

	// Stopping a module that was never started should not panic or block.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// done channel is never closed without Start, so we expect the Stop to
	// return when the context is cancelled. The module should handle nil stream.
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Stop(ctx)
	}()

	select {
	case err := <-errCh:
		// nil error is fine since there is no stream, but ctx.Err() is also acceptable.
		if err != nil && err != context.DeadlineExceeded {
			t.Errorf("Stop() returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Stop() blocked longer than expected")
	}
}

func TestStreamModule_Config_IsStored(t *testing.T) {
	config := map[string]any{
		"input":  map[string]any{"http_server": map[string]any{"address": ":8080"}},
		"output": map[string]any{"drop": map[string]any{}},
	}

	m, _ := newStreamModule("test", config)

	if m.config == nil {
		t.Error("streamModule.config is nil")
	}
	if _, ok := m.config["input"]; !ok {
		t.Error("streamModule.config missing 'input' key")
	}
	if _, ok := m.config["output"]; !ok {
		t.Error("streamModule.config missing 'output' key")
	}
}

func TestStreamModule_DoneChannel_IsInitialized(t *testing.T) {
	m, _ := newStreamModule("test", map[string]any{})
	if m.done == nil {
		t.Error("streamModule.done channel should be initialized")
	}
}
