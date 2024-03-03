package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestReadMessages(t *testing.T) {
	expectedMessages := generateTestMessages(2)
	testData, _ := json.Marshal(expectedMessages)

	messages, err := readMessages(bytes.NewReader(testData))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(messages) != len(expectedMessages) {
		t.Errorf("expected %d messages, got %d", len(expectedMessages), len(messages))
	}
	for i, msg := range messages {
		if *expectedMessages[i].MessageID != *msg.MessageID {
			t.Errorf("expected MessageId %v, got %v", expectedMessages[i].MessageID, msg.MessageID)
		}
	}
}

func TestInvalidMessages(t *testing.T) {
	tests := []struct {
		name     string
		messages string
		errStr   string
	}{
		{
			name:     "Test with empty message",
			messages: "[]",
		},
		{
			name:     "Test with invalid JSON",
			messages: `[{"body": "invalid json", "contentType": "application/json"}]`,
		},
		{
			name:     "Test with missing body",
			messages: `[{"contentType": "application/json"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testData, _ := json.Marshal(tt.messages)
			_, err := readMessages(bytes.NewReader(testData))
			if err == nil {
				t.Errorf("expected error, got nil")
				return
			}
		})
	}
}
