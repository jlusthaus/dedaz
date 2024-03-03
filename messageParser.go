package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// readMessages reads messages from the provided file and returns a slice of Message objects.
// It takes an io.Reader as input and returns the parsed messages and any error encountered.
func readMessages(file io.Reader) ([]Message, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var messages []Message
	err = json.Unmarshal(data, &messages)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("Must provide at least one message to send")
	}

	for _, message := range messages {
		err := validateMessage(message)
		if err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func validateMessage(message Message) error {
	var errBuilder strings.Builder
	if message.Body == nil {
		errBuilder.WriteString("All messages must have a body")
	}

	if message.ContentType != nil && isJsonContentType(*message.ContentType) {
		if !isValidJSON(message.Body) {
			errBuilder.WriteString("Invalid JSON in message body")
		}
	}

	if errBuilder.Len() > 0 {
		return fmt.Errorf("Invalid message detected: %s", errBuilder.String())
	}

	return nil
}

func isJsonContentType(contentType string) bool {
	contentType = strings.TrimSpace(strings.ToLower(contentType))
	return contentType == "application/json"
}

func isValidJSON(body []byte) bool {
	
	return json.Valid(body)
}
