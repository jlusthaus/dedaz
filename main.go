package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kong"
)

type CLI struct {
	Globals
	Peek   PeekCmd   `cmd:"" help:"peek deadletter messages"`
	Delete DeleteCmd `cmd:"" help:"delete messages from DLQ"`
	Resend ResendCmd `cmd:"" help:"resend messages from DLQ to the main queue"`
	Send   SendCmd   `cmd:"" help:"send messages to the main queue"`
	Seed   SeedCmd   `cmd:"" hidden:"" help:"Sends test random messages to queue and deadletters them [Testing purposes only]"`
	Info   InfoCmd   `cmd:"" help:"get information about the queue"`
}

func (c *CLI) AfterApply(ctx *kong.Context) error {
	client, err := NewClient(c.Globals.Namespace)
	if err != nil {
		return fmt.Errorf("Could not create service bus client: %v", err)
	}

	ctx.Bind(client)
	return nil
}

func main() {
	cli := CLI{}
	ctx := kong.Parse(&cli,
		kong.Name("dedaz"),
		kong.Description("Ded-simple admin tool for Azure Service Bus deadletter queues"),
		kong.UsageOnError())
	ctx.BindTo(context.Background(), (*context.Context)(nil))
	err := ctx.Run(&cli.Globals)
	ctx.FatalIfErrorf(err)
}
