package internal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewStreamModule(t *testing.T) {
	tests := []struct {
		name    string
		modName string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "valid config",
			modName: "test-stream",
			config: map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": `root = {"hello": "world"}`,
						"count":   1,
					},
				},
				"output": map[string]any{
					"drop": map[string]any{},
				},
			},
			wantErr: false,
		},
		{
			name:    "empty config",
			modName: "empty-stream",
			config:  map[string]any{},
			wantErr: false, // newStreamModule doesn't validate yet
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newStreamModule(tt.modName, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("newStreamModule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if m.name != tt.modName {
					t.Errorf("expected name %q, got %q", tt.modName, m.name)
				}
			}
		})
	}
}

func TestStreamModule_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name: "valid config",
			config: map[string]any{
				"input":  map[string]any{"generate": map[string]any{}},
				"output": map[string]any{"drop": map[string]any{}},
			},
			wantErr: false,
		},
		{
			name:    "empty config",
			config:  map[string]any{},
			wantErr: true, // Init should fail on empty config
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newStreamModule("test", tt.config)
			err := m.Init()
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStreamModule_StartStop(t *testing.T) {
	t.Run("start and stop", func(t *testing.T) {
		m, err := newStreamModule("test-stream", map[string]any{
			"input": map[string]any{
				"generate": map[string]any{
					"mapping":  `root = {"test": "data"}`,
					"count":    2,
					"interval": "100ms",
				},
			},
			"output": map[string]any{
				"drop": map[string]any{},
			},
		})
		if err != nil {
			t.Fatalf("newStreamModule() error = %v", err)
		}

		if err := m.Init(); err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		ctx := context.Background()
		if err := m.Start(ctx); err != nil {
			t.Fatalf("Start() error = %v", err)
		}

		// Let it run briefly
		time.Sleep(50 * time.Millisecond)

		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := m.Stop(stopCtx); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	t.Run("invalid YAML config", func(t *testing.T) {
		m, err := newStreamModule("bad-stream", map[string]any{
			"input": map[string]any{
				"unknown_input_type": map[string]any{
					"invalid": "config",
				},
			},
			"output": map[string]any{
				"drop": map[string]any{},
			},
		})
		if err != nil {
			t.Fatalf("newStreamModule() error = %v", err)
		}

		if err := m.Init(); err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		ctx := context.Background()
		// Start should fail with invalid config
		if err := m.Start(ctx); err == nil {
			t.Error("Start() expected error for invalid config, got nil")
			_ = m.Stop(context.Background())
		}
	})
}

func TestStreamModule_StopWithoutStart(t *testing.T) {
	m, _ := newStreamModule("test", map[string]any{"input": map[string]any{}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Stop without Start should return context deadline (done chan never closed)
	err := m.Stop(ctx)
	if err == nil {
		// If Stop completes without error, that's also acceptable
		return
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop() without Start: expected DeadlineExceeded, got %v", err)
	}
}
