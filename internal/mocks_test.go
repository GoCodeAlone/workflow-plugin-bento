package internal

// mockPublisher is a test double for sdk.MessagePublisher.
type mockPublisher struct {
	published []struct {
		topic    string
		payload  []byte
		metadata map[string]string
	}
}

func (p *mockPublisher) Publish(topic string, payload []byte, metadata map[string]string) (string, error) {
	p.published = append(p.published, struct {
		topic    string
		payload  []byte
		metadata map[string]string
	}{topic, payload, metadata})
	return "msg-id", nil
}

// mockSubscriber is a test double for sdk.MessageSubscriber.
type mockSubscriber struct {
	subscribed   map[string]func(payload []byte, metadata map[string]string) error
	unsubscribed []string
}

func (s *mockSubscriber) Subscribe(topic string, handler func(payload []byte, metadata map[string]string) error) error {
	if s.subscribed == nil {
		s.subscribed = make(map[string]func(payload []byte, metadata map[string]string) error)
	}
	s.subscribed[topic] = handler
	return nil
}

func (s *mockSubscriber) Unsubscribe(topic string) error {
	s.unsubscribed = append(s.unsubscribed, topic)
	return nil
}
