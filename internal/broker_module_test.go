package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewBrokerModule(t *testing.T) {
	tests := []struct {
		name    string
		modName string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "valid with memory transport",
			modName: "test-broker",
			config: map[string]any{
				"transport": "memory",
			},
			wantErr: false,
		},
		{
			name:    "empty config defaults to memory",
			modName: "default-broker",
			config:  map[string]any{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newBrokerModule(tt.modName, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("newBrokerModule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && m.name != tt.modName {
				t.Errorf("expected name %q, got %q", tt.modName, m.name)
			}
		})
	}
}

func TestBrokerModule_Init(t *testing.T) {
	tests := []struct {
		name          string
		config        map[string]any
		wantErr       bool
		wantTransport string
	}{
		{
			name: "explicit memory transport",
			config: map[string]any{
				"transport": "memory",
			},
			wantErr:       false,
			wantTransport: "memory",
		},
		{
			name:          "defaults to memory",
			config:        map[string]any{},
			wantErr:       false,
			wantTransport: "memory",
		},
		{
			name: "with transport_config",
			config: map[string]any{
				"transport": "memory",
				"transport_config": map[string]any{
					"limit": 100,
				},
			},
			wantErr:       false,
			wantTransport: "memory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newBrokerModule("test", tt.config)
			err := m.Init()
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && m.transport != tt.wantTransport {
				t.Errorf("expected transport %q, got %q", tt.wantTransport, m.transport)
			}
		})
	}
}

func TestBrokerModule_SetMessagePublisherAndSubscriber(t *testing.T) {
	m, _ := newBrokerModule("test", map[string]any{})

	pub := &mockMessagePublisher{}
	sub := newMockMessageSubscriber()

	m.SetMessagePublisher(pub)
	m.SetMessageSubscriber(sub)

	// Just verify it doesn't panic - can't directly compare interface values
}

func TestBrokerModule_StartStop(t *testing.T) {
	m, _ := newBrokerModule("test-broker", map[string]any{
		"transport": "memory",
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	ctx := context.Background()

	// Start is a no-op for broker
	if err := m.Start(ctx); err != nil {
		t.Errorf("Start() error = %v", err)
	}

	// Stop should work even with no streams
	if err := m.Stop(ctx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

func TestBrokerModule_EnsureStream(t *testing.T) {
	m, _ := newBrokerModule("test-broker", map[string]any{
		"transport": "memory",
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	pub := &mockMessagePublisher{}
	m.SetMessagePublisher(pub)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Create first stream for topic
	stream1, err := m.ensureStream(ctx, "topic1")
	if err != nil {
		t.Fatalf("ensureStream() error = %v", err)
	}
	if stream1 == nil {
		t.Fatal("expected non-nil stream")
	}

	// Second call should return same stream
	stream2, err := m.ensureStream(ctx, "topic1")
	if err != nil {
		t.Fatalf("ensureStream() second call error = %v", err)
	}
	if stream1 != stream2 {
		t.Error("expected same stream instance for same topic")
	}

	// Different topic should get different stream
	stream3, err := m.ensureStream(ctx, "topic2")
	if err != nil {
		t.Fatalf("ensureStream() for topic2 error = %v", err)
	}
	if stream3 == stream1 {
		t.Error("expected different stream for different topic")
	}

	// Verify streams are tracked
	m.mu.RLock()
	streamCount := len(m.streams)
	m.mu.RUnlock()

	if streamCount != 2 {
		t.Errorf("expected 2 streams, got %d", streamCount)
	}

	// Stop should clean up all streams
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	m.mu.RLock()
	streamCountAfterStop := len(m.streams)
	m.mu.RUnlock()

	if streamCountAfterStop != 0 {
		t.Errorf("expected 0 streams after stop, got %d", streamCountAfterStop)
	}
}

func TestBrokerModule_ConcurrentEnsureStream(t *testing.T) {
	m, _ := newBrokerModule("test-broker", map[string]any{
		"transport": "memory",
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	pub := &mockMessagePublisher{}
	m.SetMessagePublisher(pub)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Concurrent access to same topic
	const goroutines = 10
	var wg sync.WaitGroup
	streams := make([]*struct {
		err       error
		streamPtr interface{}
	}, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			stream, err := m.ensureStream(ctx, "concurrent-topic")
			streams[i] = &struct {
				err       error
				streamPtr interface{}
			}{err, stream}
		}()
	}

	wg.Wait()

	// All should succeed with same stream
	var firstStream interface{}
	for i, result := range streams {
		if result.err != nil {
			t.Errorf("goroutine %d: ensureStream() error = %v", i, result.err)
		}
		if i == 0 {
			firstStream = result.streamPtr
		} else if result.streamPtr != firstStream {
			t.Errorf("goroutine %d: got different stream instance", i)
		}
	}

	// Should only have one stream despite concurrent calls
	m.mu.RLock()
	streamCount := len(m.streams)
	m.mu.RUnlock()

	if streamCount != 1 {
		t.Errorf("expected 1 stream from concurrent access, got %d", streamCount)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}

func TestBrokerModule_EnsureStreamWithoutPublisher(t *testing.T) {
	m, _ := newBrokerModule("test-broker", map[string]any{
		"transport": "memory",
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// No publisher set

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Should still create stream, just without consumer func
	stream, err := m.ensureStream(ctx, "topic-no-pub")
	if err != nil {
		t.Fatalf("ensureStream() error = %v", err)
	}
	if stream == nil {
		t.Error("expected non-nil stream even without publisher")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
