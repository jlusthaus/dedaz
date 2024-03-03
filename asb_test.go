package main

import (
	"context"
	"os"
	"testing"
)

const (
	// TestQueueName is the name of the queue to use for testing
	TestNamespace = "DEDAZ_TEST_NAMESPACE"
	TestQueueName = "DEDAZ_TEST_QUEUE_NAME"
)

func TestSendMessagesIntegration(t *testing.T) {
	client, queueName := setup(t)

	testMessages := generateTestMessages(100)

	res, err := client.SendMessages(context.Background(), queueName, testMessages)
	if err != nil {
		t.Errorf("Error sending messages to queue %s: %v", queueName, err)
	}

	if res != len(testMessages) {
		t.Errorf("Failed to send all messages to queue %s", queueName)
	}
}

func TestGetAndDeleteMessagesIntegration(t *testing.T) {
	client, queueName := setup(t)
	expectedMsgCount := 250
	testMessages := generateTestMessages(expectedMsgCount)
	seededCount, err := client.SeedTestMessages(context.Background(), queueName, testMessages)
	if err != nil {
		t.Errorf("Error seeding messages to queue %s: %v", queueName, err)
	}

	if seededCount != expectedMsgCount {
		t.Errorf("Failed to seed all messages to queue %s", queueName)
	}

	ctx := context.Background()

	messages, err := client.PeekMessages(ctx, queueName)

	if err != nil {
		t.Fatalf("Could not peek messages: %v", err)
	}

	if len(messages) != expectedMsgCount {
		t.Errorf("Expected %d messages, got %d", expectedMsgCount, len(messages))
	}
	sequenceNumbers := make([]int64, 0, len(messages))
	for _, msg := range messages {
		sequenceNumbers = append(sequenceNumbers, *msg.SequenceNumber)
	}

	deletedMsgs, err := client.DeleteMessages(ctx, queueName, sequenceNumbers)

	if err != nil {
		t.Fatalf("Could not delete messages from queue %s: %v", queueName, err)
	}

	if len(deletedMsgs) != len(messages) {
		t.Errorf("Expected %d messages to be deleted, got %d", len(messages), len(deletedMsgs))
	}
}

func setup(t *testing.T) (*AsbClient, string) {
	ns := os.Getenv(TestNamespace)
	qn := os.Getenv(TestQueueName)

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	if ns == "" || qn == "" {
		t.Skipf("Skipping integration test; %s and %s must be set to run integration tests", TestNamespace, TestQueueName)
	}
	client, err := NewClient(ns)
	if err != nil {
		t.Fatalf("Unexpected error when setting up client on namespace %s: %v", ns, err)
	}

	ctx := context.Background()
	props, err := client.getQueueRuntimeProperties(ctx, qn)
	if err != nil {
		t.Fatalf("Unexpected error when accessing queue %s on namespace %s: %v", qn, ns, err)
	}

	if props == nil {
		t.Fatalf("Could not find queue %s on namespace %s", qn, ns)
	}

	activeMessages := int(props.ActiveMessageCount)
	deadLetteredMessages := int(props.DeadLetterMessageCount)

	t.Logf("Active messages: %d, Deadlettered messages: %d", activeMessages, deadLetteredMessages)
	if activeMessages != 0 {
		mainPurgedCount, err := client.purgeQueue(ctx, qn, activeMessages, MainQueue)
		if err != nil {
			t.Fatalf("Failed to purge active messages: %v", err)
		}
		if mainPurgedCount < activeMessages {
			t.Fatalf("Failed to purge all active messages; got %d, expected %d", mainPurgedCount, activeMessages)
		}
	}

	if deadLetteredMessages != 0 {
		dlqPurgedCount, err := client.purgeQueue(ctx, qn, deadLetteredMessages, DeadLetterQueue)
		if err != nil || dlqPurgedCount != deadLetteredMessages {
			t.Fatalf("Could not delete messages from deadletter queue: %v", err)
		}
	}

	return client, qn
}
