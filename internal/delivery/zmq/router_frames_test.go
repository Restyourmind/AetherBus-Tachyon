package zmq

import "testing"

func TestParseFrames(t *testing.T) {
	tests := []struct {
		name         string
		msg          [][]byte
		wantClientID string
		wantTopic    string
		wantPayload  string
		wantErr      bool
	}{
		{
			name:         "three-frame message",
			msg:          [][]byte{[]byte("cid"), []byte("user.created"), []byte("payload")},
			wantClientID: "cid",
			wantTopic:    "user.created",
			wantPayload:  "payload",
			wantErr:      false,
		},
		{
			name:         "four-frame message with delimiter",
			msg:          [][]byte{[]byte("cid"), []byte(""), []byte("user.created"), []byte("payload")},
			wantClientID: "cid",
			wantTopic:    "user.created",
			wantPayload:  "payload",
			wantErr:      false,
		},
		{
			name:    "invalid shape",
			msg:     [][]byte{[]byte("cid"), []byte("payload")},
			wantErr: true,
		},
		{
			name:    "invalid extended shape with extra frames",
			msg:     [][]byte{[]byte("cid"), []byte(""), []byte("user.created"), []byte("payload"), []byte("extra")},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientID, topic, payload, err := parseFrames(tc.msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("expected err=%v, got err=%v", tc.wantErr, err)
			}
			if err != nil {
				return
			}

			if string(clientID) != tc.wantClientID {
				t.Fatalf("expected clientID %q, got %q", tc.wantClientID, string(clientID))
			}
			if topic != tc.wantTopic {
				t.Fatalf("expected topic %q, got %q", tc.wantTopic, topic)
			}
			if string(payload) != tc.wantPayload {
				t.Fatalf("expected payload %q, got %q", tc.wantPayload, string(payload))
			}
		})
	}
}

func TestValidateTopic(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "valid topic", topic: "orders.created", wantErr: false},
		{name: "empty topic", topic: "", wantErr: true},
		{name: "leading dot", topic: ".orders", wantErr: true},
		{name: "trailing dot", topic: "orders.", wantErr: true},
		{name: "double dot", topic: "orders..created", wantErr: true},
		{name: "contains whitespace", topic: "orders. created", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTopic(tc.topic)
			if (err != nil) != tc.wantErr {
				t.Fatalf("expected err=%v, got err=%v", tc.wantErr, err)
			}
		})
	}
}
