package internal

import (
	"context"
	"encoding/json"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

// processorStep wraps Bento processors as a workflow pipeline step.
type processorStep struct {
	name       string
	config     map[string]any
	processors string // YAML for the processors section
}

func newProcessorStep(name string, config map[string]any) (*processorStep, error) {
	s := &processorStep{name: name, config: config}

	// Extract the "processors" key if present; it can be a YAML string or a map.
	switch v := config["processors"].(type) {
	case string:
		s.processors = v
	case map[string]any:
		yaml, err := configToYAML(v)
		if err != nil {
			return nil, fmt.Errorf("step.bento %q: marshal processors: %w", name, err)
		}
		s.processors = yaml
	case nil:
		// No processors — pass-through.
		s.processors = ""
	default:
		return nil, fmt.Errorf("step.bento %q: processors must be a YAML string or map", name)
	}

	return s, nil
}

// Execute runs the input data through the configured Bento processors and
// returns the transformed output.
func (s *processorStep) Execute(ctx context.Context, triggerData map[string]any, _ map[string]map[string]any, current map[string]any, _ map[string]any) (*sdk.StepResult, error) {
	// Merge current + triggerData as the step input.
	input := make(map[string]any, len(triggerData)+len(current))
	for k, v := range triggerData {
		input[k] = v
	}
	for k, v := range current {
		input[k] = v
	}

	// If no processors configured, pass data through unchanged.
	if s.processors == "" {
		return &sdk.StepResult{Output: input}, nil
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("step.bento %q: marshal input: %w", s.name, err)
	}

	// Build a one-shot stream: producer → processors → consumer.
	builder := service.NewStreamBuilder()
	builder.DisableLinting()

	producerFn, err := builder.AddProducerFunc()
	if err != nil {
		return nil, fmt.Errorf("step.bento %q: add producer: %w", s.name, err)
	}

	if err := builder.AddProcessorYAML(s.processors); err != nil {
		return nil, fmt.Errorf("step.bento %q: add processor yaml: %w", s.name, err)
	}

	// Collect results via consumer.
	resultCh := make(chan map[string]any, 1)
	errCh := make(chan error, 1)

	if err := builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		raw, err := msg.AsBytes()
		if err != nil {
			errCh <- fmt.Errorf("read processed message: %w", err)
			return err
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			// Not JSON — store raw string under "output".
			out = map[string]any{"output": string(raw)}
		}
		resultCh <- out
		return nil
	}); err != nil {
		return nil, fmt.Errorf("step.bento %q: add consumer: %w", s.name, err)
	}

	stream, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("step.bento %q: build stream: %w", s.name, err)
	}

	// Run stream in background; stop it once we've received the result.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.Run(streamCtx)
	}()

	// Send the input message.
	inputMsg := service.NewMessage(inputBytes)
	if err := producerFn(ctx, inputMsg); err != nil {
		streamCancel()
		return nil, fmt.Errorf("step.bento %q: send input message: %w", s.name, err)
	}

	// Wait for result or error.
	var output map[string]any
	select {
	case output = <-resultCh:
	case err := <-errCh:
		streamCancel()
		return nil, err
	case <-ctx.Done():
		streamCancel()
		return nil, ctx.Err()
	}

	// Stop the stream gracefully.
	if stopErr := stream.Stop(ctx); stopErr != nil {
		_ = stopErr // best-effort
	}
	streamCancel()
	<-streamDone

	return &sdk.StepResult{Output: output}, nil
}
