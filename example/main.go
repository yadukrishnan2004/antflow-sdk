package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	sdk "github.com/yadukrishnan2004/antflow-sdk"
)

// Example Workflow/Activity function
func ReverseStringWorkflow(ctx context.Context, input []byte) ([]byte, error) {
	log.Printf("Executing ReverseStringWorkflow with input: %s", string(input))
	str := string(input)

	// reverse string
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	result := string(runes)
	log.Printf("Result: %s", result)
	return []byte(result), nil
}

func ToUpperWorkflow(ctx context.Context, input []byte) ([]byte, error) {
	log.Printf("Executing ToUpperWorkflow with input: %s", string(input))
	str := string(input)
	result := strings.ToUpper(str)
	log.Printf("Result: %s", result)
	return []byte(result), nil
}

func main() {
	target := "localhost:50051"
	taskQueue := "default"

	// 1. Setup Registry

	registry := sdk.NewRegistry()
	registry.RegisterWorkflow("ProcessString").
		AddStep("Reverse", ReverseStringWorkflow).
		AddStep("ToUpper", ToUpperWorkflow)

	// 2. Start Worker in background
	worker := sdk.NewWorker(sdk.WorkerOptions{
		Target:    target,
		TaskQueue: taskQueue,
		Registry:  registry,
		WorkerID:  "worker-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := worker.Start(ctx); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}
	}()

	// Give worker a second to connect
	time.Sleep(1 * time.Second)

	// 3. Client Submission
	client, err := sdk.NewClient(target)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Register workflow definition on server (this creates the workflow record in our simple DB)
	wfID, err := client.RegisterWorkflow(ctx, "ProcessString")
	if err != nil && !strings.Contains(err.Error(), "already exists") { // Ignore if it exists in a robust setup
		log.Printf("Registering workflow... %v", err)
	}

	// Start Workflow
	inputData := []byte("Hello Temporal!")
	taskID, err := client.StartWorkflow(ctx, wfID, taskQueue, inputData)
	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	fmt.Printf("Successfully started workflow (ID: %s) and created Task (ID: %s)\n", wfID, taskID)

	// Give time for worker to process before exiting
	time.Sleep(3 * time.Second)
	fmt.Println("Example finished.")
}
