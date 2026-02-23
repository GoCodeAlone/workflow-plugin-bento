package internal

import (
	"testing"
)

func TestNewBentoTrigger_EmptyConfig(t *testing.T) {
	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(map[string]any{}, cb)
	if err != nil {
		t.Fatalf("newBentoTrigger() with empty config returned error: %v", err)
	}
	if trigger == nil {
		t.Fatal("newBentoTrigger() returned nil")
	}
}

func TestNewBentoTrigger_WithValidSubscriptions(t *testing.T) {
	config := map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input": map[string]any{
					"generate": map[string]any{
						"mapping":  "root = {}",
						"count":    1,
						"interval": "1s",
					},
				},
				"workflow": "my-workflow",
				"action":   "on_message",
			},
		},
	}

	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(config, cb)
	if err != nil {
		t.Fatalf("newBentoTrigger() returned error: %v", err)
	}
	if trigger == nil {
		t.Fatal("newBentoTrigger() returned nil")
	}

	if len(trigger.subscriptions) != 1 {
		t.Errorf("subscriptions count = %d, want 1", len(trigger.subscriptions))
	}
}

func TestNewBentoTrigger_SubscriptionsNotAList_ReturnsError(t *testing.T) {
	config := map[string]any{
		"subscriptions": "not-a-list",
	}

	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(config, cb)
	if err == nil {
		t.Fatal("newBentoTrigger() with invalid subscriptions type should return error")
	}
	if trigger != nil {
		t.Error("newBentoTrigger() with invalid subscriptions type should return nil trigger")
	}
}

func TestNewBentoTrigger_SubscriptionNotAMap_ReturnsError(t *testing.T) {
	config := map[string]any{
		"subscriptions": []any{
			"not-a-map",
		},
	}

	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(config, cb)
	if err == nil {
		t.Fatal("newBentoTrigger() with subscription as non-map should return error")
	}
	if trigger != nil {
		t.Error("newBentoTrigger() should return nil trigger on error")
	}
}

func TestNewBentoTrigger_SubscriptionMissingInput_ReturnsError(t *testing.T) {
	config := map[string]any{
		"subscriptions": []any{
			map[string]any{
				// "input" key intentionally missing
				"workflow": "my-workflow",
				"action":   "on_message",
			},
		},
	}

	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(config, cb)
	if err == nil {
		t.Fatal("newBentoTrigger() with missing input should return error")
	}
	if trigger != nil {
		t.Error("newBentoTrigger() should return nil trigger on error")
	}
}

func TestNewBentoTrigger_CallbackIsSet(t *testing.T) {
	called := false
	cb := func(action string, data map[string]any) error {
		called = true
		return nil
	}

	trigger, err := newBentoTrigger(map[string]any{}, cb)
	if err != nil {
		t.Fatalf("newBentoTrigger() returned error: %v", err)
	}

	if trigger.callback == nil {
		t.Fatal("trigger.callback should not be nil")
	}

	// Verify the callback is the one we passed in.
	_ = trigger.callback("test", nil)
	if !called {
		t.Error("trigger.callback did not call the provided callback function")
	}
}

func TestNewBentoTrigger_DoneChannel_IsInitialized(t *testing.T) {
	cb := func(action string, data map[string]any) error { return nil }
	trigger, _ := newBentoTrigger(map[string]any{}, cb)

	if trigger.done == nil {
		t.Error("trigger.done channel should be initialized")
	}
}

func TestBentoTrigger_MultipleSubscriptions(t *testing.T) {
	config := map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input":    map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
				"workflow": "workflow-1",
				"action":   "on_message",
			},
			map[string]any{
				"input":    map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
				"workflow": "workflow-2",
				// action defaults to "trigger"
			},
		},
	}

	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(config, cb)
	if err != nil {
		t.Fatalf("newBentoTrigger() returned error: %v", err)
	}

	if len(trigger.subscriptions) != 2 {
		t.Fatalf("subscriptions count = %d, want 2", len(trigger.subscriptions))
	}

	// First subscription.
	if trigger.subscriptions[0].workflow != "workflow-1" {
		t.Errorf("subscriptions[0].workflow = %q, want %q", trigger.subscriptions[0].workflow, "workflow-1")
	}
	if trigger.subscriptions[0].action != "on_message" {
		t.Errorf("subscriptions[0].action = %q, want %q", trigger.subscriptions[0].action, "on_message")
	}

	// Second subscription — action defaults to "trigger".
	if trigger.subscriptions[1].workflow != "workflow-2" {
		t.Errorf("subscriptions[1].workflow = %q, want %q", trigger.subscriptions[1].workflow, "workflow-2")
	}
	if trigger.subscriptions[1].action != "trigger" {
		t.Errorf("subscriptions[1].action = %q, want %q (default)", trigger.subscriptions[1].action, "trigger")
	}
}

func TestBentoTrigger_SubscriptionAction_DefaultsToTrigger(t *testing.T) {
	config := map[string]any{
		"subscriptions": []any{
			map[string]any{
				"input":    map[string]any{"generate": map[string]any{"mapping": "root = {}", "count": 1}},
				"workflow": "wf",
				// no "action" key
			},
		},
	}

	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(config, cb)
	if err != nil {
		t.Fatalf("newBentoTrigger() returned error: %v", err)
	}

	if trigger.subscriptions[0].action != "trigger" {
		t.Errorf("default action = %q, want %q", trigger.subscriptions[0].action, "trigger")
	}
}

func TestBentoTrigger_NoSubscriptions_IsValid(t *testing.T) {
	// A trigger with no subscriptions is valid (passive mode).
	cb := func(action string, data map[string]any) error { return nil }

	trigger, err := newBentoTrigger(map[string]any{}, cb)
	if err != nil {
		t.Fatalf("newBentoTrigger() with no subscriptions returned error: %v", err)
	}

	if len(trigger.subscriptions) != 0 {
		t.Errorf("subscriptions count = %d, want 0", len(trigger.subscriptions))
	}
}
