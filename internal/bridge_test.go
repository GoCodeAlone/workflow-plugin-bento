package internal

import (
	"testing"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

func TestConfigToYAML_SimpleMap(t *testing.T) {
	config := map[string]any{
		"key": "value",
	}

	out, err := configToYAML(config)
	if err != nil {
		t.Fatalf("configToYAML() returned error: %v", err)
	}
	if out == "" {
		t.Error("configToYAML() returned empty string")
	}
	if out != "key: value\n" {
		t.Errorf("configToYAML() = %q, want %q", out, "key: value\n")
	}
}

func TestConfigToYAML_NestedMap(t *testing.T) {
	config := map[string]any{
		"input": map[string]any{
			"generate": map[string]any{
				"mapping": "root = {}",
				"count":   1,
			},
		},
	}

	out, err := configToYAML(config)
	if err != nil {
		t.Fatalf("configToYAML() returned error: %v", err)
	}
	if out == "" {
		t.Error("configToYAML() returned empty string")
	}
}

func TestConfigToYAML_EmptyMap(t *testing.T) {
	out, err := configToYAML(map[string]any{})
	if err != nil {
		t.Fatalf("configToYAML() with empty map returned error: %v", err)
	}
	// Empty map should produce "{}\n" in YAML.
	if out == "" {
		t.Error("configToYAML() with empty map returned empty string")
	}
}

func TestMessageToMap_JSONBody(t *testing.T) {
	msg := service.NewMessage([]byte(`{"key":"value","number":42}`))

	result, err := messageToMap(msg)
	if err != nil {
		t.Fatalf("messageToMap() returned error: %v", err)
	}

	body, ok := result["body"].(map[string]any)
	if !ok {
		t.Fatalf("body should be a map, got %T", result["body"])
	}

	if body["key"] != "value" {
		t.Errorf("body[key] = %v, want %q", body["key"], "value")
	}
}

func TestMessageToMap_NonJSONBody(t *testing.T) {
	msg := service.NewMessage([]byte("plain text"))

	result, err := messageToMap(msg)
	if err != nil {
		t.Fatalf("messageToMap() returned error: %v", err)
	}

	body, ok := result["body"].(string)
	if !ok {
		t.Fatalf("non-JSON body should be string, got %T", result["body"])
	}
	if body != "plain text" {
		t.Errorf("body = %q, want %q", body, "plain text")
	}
}

func TestMessageToMap_MetadataIsPresent(t *testing.T) {
	msg := service.NewMessage([]byte(`{}`))
	msg.MetaSet("x-source", "test")

	result, err := messageToMap(msg)
	if err != nil {
		t.Fatalf("messageToMap() returned error: %v", err)
	}

	meta, ok := result["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata should be a map, got %T", result["metadata"])
	}

	if meta["x-source"] != "test" {
		t.Errorf("metadata[x-source] = %v, want %q", meta["x-source"], "test")
	}
}

func TestMessageToMap_EmptyMessage(t *testing.T) {
	msg := service.NewMessage([]byte{})

	result, err := messageToMap(msg)
	if err != nil {
		t.Fatalf("messageToMap() with empty message returned error: %v", err)
	}

	if result == nil {
		t.Error("messageToMap() should not return nil result")
	}
	if _, ok := result["metadata"]; !ok {
		t.Error("messageToMap() result should contain 'metadata' key")
	}
}

func TestMapToMessage_WithBody(t *testing.T) {
	data := map[string]any{
		"body": "hello world",
	}

	msg, err := mapToMessage(data)
	if err != nil {
		t.Fatalf("mapToMessage() returned error: %v", err)
	}
	if msg == nil {
		t.Fatal("mapToMessage() returned nil")
	}

	raw, err := msg.AsBytes()
	if err != nil {
		t.Fatalf("msg.AsBytes() returned error: %v", err)
	}
	if string(raw) != "hello world" {
		t.Errorf("message bytes = %q, want %q", string(raw), "hello world")
	}
}

func TestMapToMessage_WithJSONBody(t *testing.T) {
	data := map[string]any{
		"body": map[string]any{"key": "value"},
	}

	msg, err := mapToMessage(data)
	if err != nil {
		t.Fatalf("mapToMessage() returned error: %v", err)
	}
	if msg == nil {
		t.Fatal("mapToMessage() returned nil")
	}

	raw, err := msg.AsBytes()
	if err != nil {
		t.Fatalf("msg.AsBytes() returned error: %v", err)
	}
	if len(raw) == 0 {
		t.Error("message bytes should not be empty")
	}
}

func TestMapToMessage_WithoutBody_MarshalsFull(t *testing.T) {
	data := map[string]any{
		"key": "value",
	}

	msg, err := mapToMessage(data)
	if err != nil {
		t.Fatalf("mapToMessage() returned error: %v", err)
	}
	if msg == nil {
		t.Fatal("mapToMessage() returned nil")
	}

	raw, err := msg.AsBytes()
	if err != nil {
		t.Fatalf("msg.AsBytes() returned error: %v", err)
	}
	if len(raw) == 0 {
		t.Error("message bytes should not be empty")
	}
}

func TestMapToMessage_WithMetadata(t *testing.T) {
	data := map[string]any{
		"body": "payload",
		"metadata": map[string]any{
			"x-trace": "abc123",
		},
	}

	msg, err := mapToMessage(data)
	if err != nil {
		t.Fatalf("mapToMessage() returned error: %v", err)
	}
	if msg == nil {
		t.Fatal("mapToMessage() returned nil")
	}

	val, exists := msg.MetaGet("x-trace")
	if !exists {
		t.Fatal("metadata key 'x-trace' not found on message")
	}
	if val != "abc123" {
		t.Errorf("metadata[x-trace] = %q, want %q", val, "abc123")
	}
}

func TestMapToMessage_EmptyMap(t *testing.T) {
	msg, err := mapToMessage(map[string]any{})
	if err != nil {
		t.Fatalf("mapToMessage() with empty map returned error: %v", err)
	}
	if msg == nil {
		t.Fatal("mapToMessage() with empty map returned nil")
	}
}

func TestRoundTrip_MessageToMapToMessage(t *testing.T) {
	original := service.NewMessage([]byte(`{"name":"test","count":3}`))
	original.MetaSet("source", "unit-test")

	// Convert to map.
	m, err := messageToMap(original)
	if err != nil {
		t.Fatalf("messageToMap() returned error: %v", err)
	}

	// Convert back to message.
	reconstructed, err := mapToMessage(m)
	if err != nil {
		t.Fatalf("mapToMessage() returned error: %v", err)
	}
	if reconstructed == nil {
		t.Fatal("mapToMessage() returned nil")
	}
}
