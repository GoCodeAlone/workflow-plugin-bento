package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockMessagePublisher captures published messages for testing.
type mockMessagePublisher struct {
	mu       sync.Mutex
	messages []mockPublishedMessage
}

type mockPublishedMessage struct {
	topic    string
	payload  []byte
	metadata map[string]string
}

func (m *mockMessagePublisher) Publish(topic string, payload []byte, metadata map[string]string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, mockPublishedMessage{
		topic:    topic,
		payload:  append([]byte(nil), payload...),
		metadata: metadata,
	})
	return "msg-id", nil
}

func (m *mockMessagePublisher) GetMessages() []mockPublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]mockPublishedMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

func TestNewInputModule(t *testing.T) {
	tests := []struct {
		name    string
		modName string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "valid config",
			modName: "test-input",
			config: map[string]any{
				"target_topic": "test-topic",
				"input": map[string]any{
					"generate": map[string]any{
						"mapping": `root = {"hello": "world"}`,
						"count":   1,
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newInputModule(tt.modName, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("newInputModule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && m.name != tt.modName {
				t.Errorf("expected name %q, got %q", tt.modName, m.name)
			}
		})
	}
}

func TestInputModule_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name: "valid config with target_topic",
			config: map[string]any{
				"target_topic": "my-topic",
				"input": map[string]any{
					"generate": map[string]any{},
				},
			},
			wantErr: false,
		},
		{
			name: "missing target_topic",
			config: map[string]any{
				"input": map[string]any{
					"generate": map[string]any{},
				},
			},
			wantErr: true,
		},
		{
			name: "empty target_topic",
			config: map[string]any{
				"target_topic": "",
				"input": map[string]any{
					"generate": map[string]any{},
				},
			},
			wantErr: true,
		},
		{
			name: "missing input config",
			config: map[string]any{
				"target_topic": "my-topic",
			},
			wantErr: true,
		},
		{
			name: "with target_broker",
			config: map[string]any{
				"target_topic":  "my-topic",
				"target_broker": "my-broker",
				"input": map[string]any{
					"generate": map[string]any{},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newInputModule("test", tt.config)
			err := m.Init()
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInputModule_SetMessagePublisher(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{})
	pub := &mockMessagePublisher{}

	m.SetMessagePublisher(pub)

	if m.publisher != pub {
		t.Error("SetMessagePublisher() did not set publisher")
	}
}

func TestInputModule_SetMessageSubscriber(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{})
	// Should not panic - inputModule doesn't use subscriber
	m.SetMessageSubscriber(nil)
}

func TestInputModule_StartWithoutPublisher(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{
		"target_topic": "test-topic",
		"input": map[string]any{
			"generate": map[string]any{
				"mapping": `root = {"test": "data"}`,
				"count":   1,
			},
		},
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Start without publisher should error
	ctx := context.Background()
	err := m.Start(ctx)
	if err == nil {
		t.Error("Start() without publisher should error")
		_ = m.Stop(context.Background())
	}
}

func TestInputModule_PublishMessages(t *testing.T) {
	pub := &mockMessagePublisher{}

	m, _ := newInputModule("test-input", map[string]any{
		"target_topic": "test-topic",
		"input": map[string]any{
			"generate": map[string]any{
				"mapping":  `root = {"id": count("input_id"), "msg": "hello"}`,
				"count":    3,
				"interval": "10ms",
			},
		},
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	m.SetMessagePublisher(pub)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Poll until 3 messages are published or deadline is reached.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(pub.GetMessages()) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	messages := pub.GetMessages()
	if len(messages) != 3 {
		t.Errorf("expected 3 messages, got %d", len(messages))
	}

	for _, msg := range messages {
		if msg.topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %q", msg.topic)
		}
		if len(msg.payload) == 0 {
			t.Error("expected non-empty payload")
		}
	}
}

func TestInputModule_InvalidInputConfig(t *testing.T) {
	m, _ := newInputModule("test", map[string]any{
		"target_topic": "test-topic",
		"input":        "not-a-map", // invalid: should be map
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	pub := &mockMessagePublisher{}
	m.SetMessagePublisher(pub)

	ctx := context.Background()
	// Start should fail with invalid input config type
	err := m.Start(ctx)
	if err == nil {
		t.Error("Start() expected error for invalid input config type")
		_ = m.Stop(context.Background())
	}
}
