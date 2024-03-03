package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type PeekCmd struct{}

func (g *PeekCmd) Run(ctx context.Context, globals *Globals, client *AsbClient) error {
	messages, err := client.PeekMessages(ctx, globals.QueueName)

	if err != nil {
		return err
	}
	err = outputAsJson(messages)

	if err != nil {
		return err
	}
	return nil
}

type DeleteCmd struct {
	SequenceNumbers []int64 `arg:"" required:"" help:"Sequence Numbers of messages to remove from DLQ"`
}

func (d *DeleteCmd) Run(ctx context.Context, globals *Globals, client *AsbClient) error {
	log.Printf("Deleting messages: %v", len(d.SequenceNumbers))
	messages, err := client.DeleteMessages(ctx, globals.QueueName, d.SequenceNumbers)
	if err != nil {
		return err
	}

	if err = outputAsJson(messages); err != nil {
		return err
	}
	
	return nil
}

type ResendCmd struct {
	SequenceNumbers []int64 `arg:"" required:"" help:"Sequence Numbers of messages to resend from DLQ to the main queue"`
}

func (r *ResendCmd) Run(ctx context.Context, globals *Globals, client *AsbClient) error {
	messages, err := client.ResendMessages(ctx, globals.QueueName, r.SequenceNumbers)

	if err != nil {
		log.Fatalf("Could not resend messages: %v", err)
	}

	if err = outputAsJson(messages); err != nil {
		return err
	}

	return nil
}

type SendCmd struct {
	FilePath *os.File `short:"f" required:"" help:"Path to the file containing the messages to send"`
}

func (r *SendCmd) Run(ctx context.Context, globals *Globals, client *AsbClient) error {
	messages, err := readMessages(r.FilePath)
	if err != nil {
		return fmt.Errorf("Could not parse messages from file: %v", err)
	}

	count, err := client.SendMessages(ctx, globals.QueueName, messages)
	if err != nil {
		return err
	}

	result := struct {
		Sent int `json:"sent"`
	}{
		Sent: count,	
	}
	
	if err = outputAsJson(result); err != nil {
		return err
	}

	return nil
}

type SeedCmd struct {
	Unsafe bool `help:"Enable unsafe mode." default:"false"`
	Number int  `short:"n" help:"Number of messages to create and send to dlq" default:"10"`
}

func (d *SeedCmd) Run(ctx context.Context, globals *Globals, client *AsbClient) error {
	if !d.Unsafe {
		return fmt.Errorf("This command is unsafe. Use --unsafe to confirm.")
	}

	testMessages := generateTestMessages(d.Number)
	count, err := client.SeedTestMessages(ctx, globals.QueueName, testMessages)

	if err != nil {
		return fmt.Errorf("Could not deadletter messages: %v", err)
	}

	log.Printf("Deadlettered %d messages", count)

	return nil
}

type InfoCmd struct{}

func (g *InfoCmd) Run(ctx context.Context, globals *Globals, client *AsbClient) error {
	properties, err := client.getQueueRuntimeProperties(ctx, globals.QueueName)

	if err != nil {
		return err
	}

	if err = outputAsJson(properties); err != nil {
		return err
	}
	return nil
}

func outputAsJson(result interface{}) error {
	json, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("Could not convert output to JSON: %v", err)
	}
	fmt.Println(string(json))
	return nil
}
