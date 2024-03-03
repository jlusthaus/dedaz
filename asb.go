package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

const (
	// ReceiveMessageMax is the maximum number of messages to receive from a queue by a single receiver
	ReceiveMessageMax = 100

	// TimeoutInterval is the default timeout interval for queue operations
	TimeoutInterval = 10 * time.Second

	// receiverCount is the maximum number of receivers to use for reading from a queue
	ReceiverCount = 2
)

type QueueType int

const (
	MainQueue       QueueType = 0
	DeadLetterQueue QueueType = 1
)

type AsbClient struct {
	client      *azservicebus.Client
	adminClient *admin.Client
	logger      *log.Logger
}

type Message struct {
	ApplicationProperties map[string]interface{} `json:"ApplicationProperties"`
	Body                  []byte                 `json:"Body"`
	ContentType           *string                `json:"ContentType"`
	CorrelationID         *string                `json:"CorrelationID"`
	MessageID             *string                `json:"MessageID"`
	PartitionKey          *string                `json:"PartitionKey"`
	ReplyTo               *string                `json:"ReplyTo"`
	ReplyToSessionID      *string                `json:"ReplyToSessionID"`
	ScheduledEnqueueTime  *time.Time             `json:"ScheduledEnqueueTime"`
	SessionID             *string                `json:"SessionID"`
	Subject               *string                `json:"Subject"`
	TimeToLive            *time.Duration         `json:"TimeToLive"`
	To                    *string                `json:"To"`
}

type DeadLetterMessage struct {
	ApplicationProperties      map[string]interface{} `json:"applicationProperties"`
	Body                       string                 `json:"body"`
	ContentType                *string                `json:"contentType"`
	CorrelationID              *string                `json:"correlationId"`
	DeadLetterErrorDescription *string                `json:"deadLetterErrorDescription"`
	DeadLetterReason           *string                `json:"deadLetterReason"`
	DeadLetterSource           *string                `json:"deadLetterSource"`
	DeliveryCount              uint32                 `json:"deliveryCount"`
	EnqueuedSequenceNumber     *int64                 `json:"enqueuedSequenceNumber"`
	EnqueuedTime               *time.Time             `json:"enqueuedTime"`
	ExpiresAt                  *time.Time             `json:"expiresAt"`
	MessageID                  string                 `json:"messageId"`
	PartitionKey               *string                `json:"partitionKey"`
	ReplyTo                    *string                `json:"replyTo"`
	ReplyToSessionID           *string                `json:"replyToSessionId"`
	ScheduledEnqueueTime       *time.Time             `json:"scheduledEnqueueTime"`
	SequenceNumber             *int64                 `json:"sequenceNumber"`
	SessionID                  *string                `json:"sessionId"`
	Subject                    *string                `json:"subject"`
	TimeToLive                 *time.Duration         `json:"timeToLive"`
	To                         *string                `json:"to"`
}

func NewClient(namespace string) (*AsbClient, error) {

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := azservicebus.NewClient(namespace, cred, nil)
	if err != nil {
		return nil, err
	}

	adminClient, err := admin.NewClient(namespace, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create admin client: %w", err)
	}

	return &AsbClient{
		client:      client,
		adminClient: adminClient,
		logger:      log.Default(),
	}, nil
}

func (c *AsbClient) PeekMessages(ctx context.Context, queueName string) ([]DeadLetterMessage, error) {
	receiver, err := c.client.NewReceiverForQueue(queueName, &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter, ReceiveMode: azservicebus.ReceiveModePeekLock})
	if err != nil {
		return nil, fmt.Errorf("Could not create queue receiver: %w", err)
	}

	defer receiver.Close(ctx)

	allMessages := []*azservicebus.ReceivedMessage{}

	for {
		messages, err := receiver.PeekMessages(ctx, ReceiveMessageMax, nil)
		if err != nil {
			return nil, fmt.Errorf("Failed peeking messages: %w", err)
		}

		if len(messages) == 0 {
			break
		}
		allMessages = append(allMessages, messages...)
	}

	return toDeadletterMessages(allMessages), nil
}

type messageHandler = func(context.Context, *azservicebus.ReceivedMessage) bool

func (c *AsbClient) DeleteMessages(ctx context.Context, queueName string, sequenceNumbers []int64) ([]DeadLetterMessage, error) {
	return c.processMessagesForCompletion(ctx, queueName, len(sequenceNumbers), func(ctx context.Context, message *azservicebus.ReceivedMessage) bool {
		return slices.Contains(sequenceNumbers, *message.SequenceNumber)
	})
}

func (c *AsbClient) ResendMessages(ctx context.Context, queueName string, sequenceNumbers []int64) ([]DeadLetterMessage, error) {
	sender, err := c.client.NewSender(queueName, nil)

	if err != nil {
		return nil, fmt.Errorf("Could not create queue sender: %w", err)
	}

	return c.processMessagesForCompletion(ctx, queueName, len(sequenceNumbers), func(ctx context.Context, message *azservicebus.ReceivedMessage) bool {
		if !slices.Contains(sequenceNumbers, *message.SequenceNumber) {
			return false
		}

		err = sender.SendMessage(ctx, message.Message(), nil)
		if err != nil {
			c.logger.Printf("Failed to resend message %d: %v", *message.SequenceNumber, err)
			return false
		}
		return true
	})
}

func (c *AsbClient) processMessagesForCompletion(ctx context.Context, queueName string, messageCount int, handler messageHandler) ([]DeadLetterMessage, error) {
	deletedMessages := []*azservicebus.ReceivedMessage{}

	resultCh := make(chan azservicebus.ReceivedMessage)
	errChannel := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < ReceiverCount; i++ {
		go c.receiveAndCompleteMessages(ctx, queueName, handler, resultCh, errChannel)
	}

	for {
		select {
		case err := <-errChannel:
			cancel()
			return nil, err
		case message := <-resultCh:
			deletedMessages = append(deletedMessages, &message)
			if len(deletedMessages) == messageCount {
				cancel()
				return toDeadletterMessages(deletedMessages), nil
			}
		}
	}
}

func (c *AsbClient) receiveAndCompleteMessages(ctx context.Context, queueName string, action messageHandler, resultChan chan<- azservicebus.ReceivedMessage, errChan chan<- error) {
	// Create a context that will not cancel operations when the parent context is cancelled. This will allow us to clean up the receiver and any received messages before returning.
	cleanUpCtx := context.WithoutCancel(ctx)
	receiver, err := c.client.NewReceiverForQueue(queueName, &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter, ReceiveMode: azservicebus.ReceiveModePeekLock})
	if err != nil {
		errChan <- err
		return
	}
	defer receiver.Close(cleanUpCtx)

	// Keep track of all incomplete messages to relinquish message lock and send back to DLQ before
	// closing receiver. This potentially makes the message available to other consumers sooner than waiting for the
	// lock to expired.
	messagesToAbandon := make([]*azservicebus.ReceivedMessage, 0)
	for {
		receivedMessages, err := receiver.ReceiveMessages(ctx, ReceiveMessageMax, nil)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			errChan <- err
			break
		}
		receivedMessagesCount := len(receivedMessages)
		c.logger.Printf("Received %d messages for deletion on receiver", receivedMessagesCount)

		for _, message := range receivedMessages {
			if action(ctx, message) {
				err := receiver.CompleteMessage(ctx, message, nil)
				if err != nil {
					// If the context is cancelled, we should stop processing messages and return.
					if errors.Is(err, context.Canceled) {
						break
					}
					c.logger.Printf("Failed to complete message %d: %v", message.SequenceNumber, err)
					messagesToAbandon = append(messagesToAbandon, message)
					continue
				}
				resultChan <- *message
			} else {
				messagesToAbandon = append(messagesToAbandon, message)
			}
		}
	}

	// If we did not finish processing all messages(e.g., in the case of context cancellation), we should abandon the rest.
	for _, messageToAbandon := range messagesToAbandon {
		err := receiver.AbandonMessage(cleanUpCtx, messageToAbandon, nil)
		if err != nil {
			c.logger.Printf("Failed to abandon message %d: %v", *messageToAbandon.SequenceNumber, err)
		}
	}
}

func (c *AsbClient) SendMessages(ctx context.Context, queueName string, messages []Message) (int, error) {
	sender, err := c.client.NewSender(queueName, nil)

	if err != nil {
		return 0, fmt.Errorf("Could not create queue sender: %w", err)
	}

	defer sender.Close(ctx)
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("Could not create first message batch: %w", err)
	}

	for _, message := range messages {
		err := batch.AddMessage(fromMessage(message), nil)
		if err != nil {
			if err == azservicebus.ErrMessageTooLarge {
				err = sender.SendMessageBatch(ctx, batch, nil)
				if err != nil {
					return 0, fmt.Errorf("Failed to send message batch: %w", err)
				}
				batch, err = sender.NewMessageBatch(ctx, nil)
				if err != nil {
					return 0, fmt.Errorf("Failed to create new message batch: %w", err)
				}
				err = batch.AddMessage(fromMessage(message), nil)
				if err != nil {
					return 0, fmt.Errorf("Failed to add message to new message batch: %w", err)
				}
			} else {
				return 0, fmt.Errorf("Failed to add message to message batch: %w", err)
			}
		}
	}

	if batch.NumMessages() > 0 {
		err = sender.SendMessageBatch(ctx, batch, nil)
		if err != nil {
			return 0, fmt.Errorf("Failed to send message batch: %w", err)
		}
	}
	return len(messages), nil
}

// deadLetterMessages is a function that transfers a specified number of messages from a queue to the deadletterqueue. If toDeadLetter is less than or equal to 0, it will attempt to transfer all
// messages. This should not be used outside of testing.
func (c *AsbClient) deadLetterMessages(ctx context.Context, queueName string, toDeadLetter int) (int, error) {
	deadLetterOptions := &azservicebus.DeadLetterOptions{
		ErrorDescription: to.Ptr("Dedaz test error description"),
		Reason:           to.Ptr("Dedaz test reason"),
	}

	receiver, err := c.client.NewReceiverForQueue(queueName, nil)
	if err != nil {
		return 0, err
	}

	deadletteredCount := 0
	for toDeadLetter <= 0 || deadletteredCount < toDeadLetter {
		toCx, cancel := context.WithTimeout(ctx, TimeoutInterval)
		defer cancel()
		c.logger.Printf("Receiving messages; total deadlettered so far: %d", deadletteredCount)
		receivedMessages, err := receiver.ReceiveMessages(toCx, ReceiveMessageMax, nil)
		if err != nil {
			break
		}

		receivedMessagesCount := len(receivedMessages)
		c.logger.Printf("Received %d messages to deadletter", receivedMessagesCount)

		if receivedMessagesCount == 0 {
			break
		}

		for _, message := range receivedMessages {
			err := receiver.DeadLetterMessage(ctx, message, deadLetterOptions)
			if err != nil {
				return 0, fmt.Errorf("Failed to deadletter message: %w", err)
			}
			deadletteredCount += 1
		}
	}

	return deadletteredCount, nil
}

// SeedTestMessages sends a batch of test messages to the specified queue and transfers to the deadletter queue. This is useful for setting up tests
// and should not be used outside of testing.
func (c *AsbClient) SeedTestMessages(ctx context.Context, queueName string, messages []Message) (int, error) {
	_, err := c.SendMessages(ctx, queueName, messages)
	if err != nil {
		return 0, fmt.Errorf("Could send test messages to queue: %v", err)
	}

	dlqCount, err := c.deadLetterMessages(ctx, queueName, len(messages))
	return dlqCount, err
}

// getQueueRuntimeProperties returns the runtime properties of the specified queue.
func (c *AsbClient) getQueueRuntimeProperties(ctx context.Context, queueName string) (*admin.GetQueueRuntimePropertiesResponse, error) {
	return c.adminClient.GetQueueRuntimeProperties(ctx, queueName, nil)
}

// purgeMainQueue purges the main queue of at least the number of messages specified by minimumToPurge.
// minimumToPurge is the minimum number of messages to purge, but the function may delete more depending on the number of messages in the batches
// each receiver receives. Until this number is reached, the function wil block and continue to receive messages
// unless the context is cancelled. Given its indiscriminate nature, this should not be used outside of testing.
func (c *AsbClient) purgeQueue(ctx context.Context, queueName string, minimumToPurge int, queueType QueueType) (int, error) {
	purgeMessages := func(ctx context.Context, resultChan chan<- int, errChannel chan<- error, num int) {
		var receiverOptions *azservicebus.ReceiverOptions
		if queueType == DeadLetterQueue {
			receiverOptions = &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter, ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
		} else {
			receiverOptions = &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
		}
		receiver, err := c.client.NewReceiverForQueue(queueName, receiverOptions)
		if err != nil {
			c.logger.Printf("Could not create queue receiver for purgeMainQueue %d: %v", num, err)
			errChannel <- err
			return
		}
		defer receiver.Close(context.WithoutCancel(ctx))

		for {
			receivedMessages, err := receiver.ReceiveMessages(ctx, ReceiveMessageMax, nil)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					break
				}
				errChannel <- err
				return
			}

			resultChan <- len(receivedMessages)
		}
	}

	resultChan := make(chan int)
	errChannel := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < ReceiverCount; i++ {
		go purgeMessages(ctx, resultChan, errChannel, i)
	}

	totalCount := 0
	for {
		select {
		case err := <-errChannel:
			cancel()
			return 0, err
		case count := <-resultChan:
			totalCount += count
			c.logger.Printf("Total received messages: %d", totalCount)
			if totalCount >= minimumToPurge {
				cancel()
				return totalCount, nil
			}
		}
	}
}

func fromMessage(message Message) *azservicebus.Message {
	return &azservicebus.Message{
		ApplicationProperties: message.ApplicationProperties,
		Body:                  message.Body,
		ContentType:           message.ContentType,
		CorrelationID:         message.CorrelationID,
		MessageID:             message.MessageID,
		PartitionKey:          message.PartitionKey,
		ReplyTo:               message.ReplyTo,
		ReplyToSessionID:      message.ReplyToSessionID,
		ScheduledEnqueueTime:  message.ScheduledEnqueueTime,
		SessionID:             message.SessionID,
		Subject:               message.Subject,
		TimeToLive:            message.TimeToLive,
		To:                    message.To,
	}
}

func toDeadletterMessages(receivedmessages []*azservicebus.ReceivedMessage) []DeadLetterMessage {
	deadletterMessages := make([]DeadLetterMessage, len(receivedmessages))
	for i, message := range receivedmessages {
		deadletterMessages[i] = DeadLetterMessage{
			ApplicationProperties:      message.ApplicationProperties,
			Body:                       string(message.Body),
			ContentType:                message.ContentType,
			CorrelationID:              message.CorrelationID,
			DeadLetterErrorDescription: message.DeadLetterErrorDescription,
			DeadLetterReason:           message.DeadLetterReason,
			DeadLetterSource:           message.DeadLetterSource,
			DeliveryCount:              message.DeliveryCount,
			EnqueuedSequenceNumber:     message.EnqueuedSequenceNumber,
			EnqueuedTime:               message.EnqueuedTime,
			ExpiresAt:                  message.ExpiresAt,
			MessageID:                  message.MessageID,
			PartitionKey:               message.PartitionKey,
			ReplyTo:                    message.ReplyTo,
			ReplyToSessionID:           message.ReplyToSessionID,
			ScheduledEnqueueTime:       message.ScheduledEnqueueTime,
			SequenceNumber:             message.SequenceNumber,
			SessionID:                  message.SessionID,
			Subject:                    message.Subject,
			TimeToLive:                 message.TimeToLive,
			To:                         message.To,
		}
	}

	return deadletterMessages
}
