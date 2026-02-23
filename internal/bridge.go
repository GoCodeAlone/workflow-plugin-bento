package internal

import (
	"encoding/json"
	"fmt"

	"github.com/warpstreamlabs/bento/v4/public/service"
	"gopkg.in/yaml.v3"
)

// configToYAML converts a map config to a YAML string suitable for Bento's SetYAML / AddInputYAML / etc.
func configToYAML(config map[string]any) (string, error) {
	data, err := yaml.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("marshal config to yaml: %w", err)
	}
	return string(data), nil
}

// messageToMap converts a Bento service.Message to a plain map.
// The message body is decoded as JSON if possible; otherwise stored as a raw string.
func messageToMap(msg *service.Message) (map[string]any, error) {
	raw, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("read message bytes: %w", err)
	}

	result := map[string]any{
		"metadata": map[string]any{},
	}

	// Attempt JSON decode of the body.
	var body any
	if jsonErr := json.Unmarshal(raw, &body); jsonErr == nil {
		result["body"] = body
	} else {
		result["body"] = string(raw)
	}

	// Copy metadata.
	meta := map[string]any{}
	if err := msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	}); err != nil {
		return nil, fmt.Errorf("copy message metadata: %w", err)
	}
	result["metadata"] = meta

	return result, nil
}

// mapToMessage converts a map to a Bento service.Message.
// If the map has a "body" key, its JSON representation becomes the message bytes.
// Additional metadata keys are attached.
func mapToMessage(data map[string]any) *service.Message {
	var body []byte

	if b, ok := data["body"]; ok {
		if s, ok2 := b.(string); ok2 {
			body = []byte(s)
		} else {
			body, _ = json.Marshal(b)
		}
	} else {
		body, _ = json.Marshal(data)
	}

	msg := service.NewMessage(body)

	if meta, ok := data["metadata"].(map[string]any); ok {
		for k, v := range meta {
			msg.MetaSetMut(k, v)
		}
	}

	return msg
}
