package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

func generateTestMessages(n int) []Message {
	messages := make([]Message, n)
	for i := range messages {
		id := rand.Int63()
		contentType := "application/json"
		correlationID := fmt.Sprintf("correlation-id-%d", id)
		// Per Azure Service Bus documentation, is an application-controlled value that should be unique.
		messageID := fmt.Sprintf("message-id-%d", id)
		subject := fmt.Sprintf("subject-%d", id)
		appProperties := map[string]interface{}{
			"key1": fmt.Sprintf("value%d", id),
			"key2": id,
		}

		bodydata := map[string]interface{}{
			"id":   rand.Intn(100),
			"name": fmt.Sprintf("name-%d", rand.Intn(100)),
		}

		body, _ := json.Marshal(bodydata)

		messages[i] = Message{
			ApplicationProperties: appProperties,
			Body:                  body,
			ContentType:           &contentType,
			CorrelationID:         &correlationID,
			MessageID:             &messageID,
			Subject:               &subject,
		}
	}
	return messages
}
