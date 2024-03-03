package main

// TODO: Implement Connection String support
type Globals struct {
	Namespace string `alias:"ns" help:"Namespace of ASB" env:"DEDAZ_NAMESPACE" required:""`
	QueueName string `alias:"qn" help:"Name of the queue" env:"DEDAZ_QUEUE" required:""`
}
