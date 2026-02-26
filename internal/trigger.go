package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

// triggerSubscription describes a single Bento input → workflow mapping.
type triggerSubscription struct {
	inputConfig map[string]any
	workflow    string
	action      string
}

// bentoTrigger fires workflow callbacks when Bento inputs receive messages.
type bentoTrigger struct {
	config        map[string]any
	callback      sdk.TriggerCallback
	subscriptions []triggerSubscription
	streams       []*service.Stream
	cancel        context.CancelFunc
	done          chan struct{}
	mu            sync.Mutex
}

func newBentoTrigger(config map[string]any, cb sdk.TriggerCallback) (*bentoTrigger, error) {
	t := &bentoTrigger{
		config:   config,
		callback: cb,
		done:     make(chan struct{}),
	}

	if err := t.parseSubscriptions(); err != nil {
		return nil, err
	}
	return t, nil
}

// parseSubscriptions reads the "subscriptions" list from config.
//
// Expected shape:
//
//	subscriptions:
//	  - input:
//	      generate:
//	        mapping: 'root = {"hello": "world"}'
//	        interval: 1s
//	        count: 0
//	    workflow: my-workflow
//	    action: on_message
func (t *bentoTrigger) parseSubscriptions() error {
	rawSubs, ok := t.config["subscriptions"]
	if !ok {
		return nil // No subscriptions configured — valid but passive.
	}

	subs, ok := rawSubs.([]any)
	if !ok {
		return fmt.Errorf("bento trigger: subscriptions must be a list")
	}

	for i, raw := range subs {
		sub, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("bento trigger: subscription[%d] must be a map", i)
		}

		inputCfg, ok := sub["input"].(map[string]any)
		if !ok {
			return fmt.Errorf("bento trigger: subscription[%d].input must be a map", i)
		}

		workflow, _ := sub["workflow"].(string)
		action, _ := sub["action"].(string)
		if action == "" {
			action = "trigger"
		}

		t.subscriptions = append(t.subscriptions, triggerSubscription{
			inputConfig: inputCfg,
			workflow:    workflow,
			action:      action,
		})
	}
	return nil
}

// Start creates one Bento input stream per subscription and runs them.
func (t *bentoTrigger) Start(ctx context.Context) error {
	slog.Info("starting bento trigger", "subscriptions", len(t.subscriptions))

	runCtx, cancel := context.WithCancel(ctx)

	t.mu.Lock()
	t.cancel = cancel
	t.mu.Unlock()

	var wg sync.WaitGroup

	for i, sub := range t.subscriptions {
		sub := sub // capture loop variable
		idx := i

		inputYAML, err := configToYAML(sub.inputConfig)
		if err != nil {
			cancel()
			return fmt.Errorf("bento trigger: marshal input config for workflow %q: %w", sub.workflow, err)
		}

		builder := service.NewStreamBuilder()
		builder.DisableLinting()
		if err := builder.AddInputYAML(inputYAML); err != nil {
			cancel()
			return fmt.Errorf("bento trigger: add input yaml for workflow %q: %w", sub.workflow, err)
		}

		cb := t.callback
		action := sub.action
		workflow := sub.workflow

		if err := builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
			data, convErr := messageToMap(msg)
			if convErr != nil {
				slog.Error("failed to convert message", "error", convErr, "workflow", workflow)
				return convErr
			}
			if workflow != "" {
				data["workflow"] = workflow
			}

			slog.Debug("trigger firing workflow", "workflow", workflow, "action", action)

			callbackErr := cb(action, data)
			if callbackErr != nil {
				slog.Error("workflow callback failed", "error", callbackErr, "workflow", workflow, "action", action)
			}
			return callbackErr
		}); err != nil {
			cancel()
			return fmt.Errorf("bento trigger: add consumer for workflow %q: %w", sub.workflow, err)
		}

		stream, err := builder.Build()
		if err != nil {
			cancel()
			return fmt.Errorf("bento trigger: build stream for workflow %q: %w", sub.workflow, err)
		}

		t.mu.Lock()
		t.streams = append(t.streams, stream)
		t.mu.Unlock()

		slog.Info("bento trigger subscription started", "index", idx, "workflow", workflow, "action", action)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
				slog.Error("bento trigger stream runtime error", "workflow", workflow, "action", action, "error", err)
				if cbErr := cb("stream_error", map[string]any{"error": err.Error(), "workflow": workflow, "action": action}); cbErr != nil {
					slog.Error("bento trigger stream_error callback error", "workflow", workflow, "action", action, "callback_error", cbErr)
				}
			}
		}()
	}

	// Close done when all goroutines exit.
	go func() {
		wg.Wait()
		close(t.done)
		slog.Info("all bento trigger streams exited")
	}()

	return nil
}

// Stop halts all running streams and waits for goroutines to finish.
func (t *bentoTrigger) Stop(ctx context.Context) error {
	slog.Info("stopping bento trigger")

	t.mu.Lock()
	streams := make([]*service.Stream, len(t.streams))
	copy(streams, t.streams)
	cancel := t.cancel
	t.mu.Unlock()

	var firstErr error
	for i, stream := range streams {
		if err := stream.Stop(ctx); err != nil {
			slog.Error("failed to stop trigger stream", "error", err, "index", i)
			if firstErr == nil {
				firstErr = fmt.Errorf("bento trigger: stop stream: %w", err)
			}
		}
	}

	if cancel != nil {
		cancel()
	}

	select {
	case <-t.done:
		slog.Info("bento trigger stopped")
	case <-ctx.Done():
		return ctx.Err()
	}

	return firstErr
}
