package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/yadukrishnan2004/antflow-sdk"
)

// AddTenWorkflow adds 10 to the integer input
func AddTenWorkflow(ctx context.Context, input []byte) ([]byte, error) {
	val, err := strconv.Atoi(string(input))
	if err != nil {
		return nil, fmt.Errorf("invalid integer input: %w", err)
	}
	log.Printf("[Step 1] AddTenWorkflow: %d + 10", val)
	result := val + 10
	return []byte(strconv.Itoa(result)), nil
}

// MultiplyByTwoWorkflow multiplies the input by 2
func MultiplyByTwoWorkflow(ctx context.Context, input []byte) ([]byte, error) {
	val, err := strconv.Atoi(string(input))
	if err != nil {
		return nil, fmt.Errorf("invalid integer input: %w", err)
	}
	log.Printf("[Step 2] MultiplyByTwoWorkflow: %d * 2", val)
	result := val * 2
	log.Printf("Final Output will be: %d", result)
	return []byte(strconv.Itoa(result)), nil
}

func main() {
	target := "localhost:50051"
	taskQueue := "math-queue"

	// 1. Setup Registry
	registry := sdk.NewRegistry()

	// Registering the sequential steps
	registry.RegisterWorkflow("MathPipeline").
		AddStep("AddTen", AddTenWorkflow).
		AddStep("MultiplyByTwo", MultiplyByTwoWorkflow)

	// 2. Start Worker in background
	worker := sdk.NewWorker(sdk.WorkerOptions{
		Target:    target,
		TaskQueue: taskQueue,
		Registry:  registry,
		WorkerID:  "math-worker-1",
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

	wfID, err := client.RegisterWorkflow(ctx, "MathPipeline")
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		log.Printf("Registering workflow... %v", err)
	}

	inputData := []byte("5") // starting number
	taskID, err := client.StartWorkflow(ctx, wfID, taskQueue, inputData)
	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	fmt.Printf("Successfully started workflow (ID: %s) and created Task (ID: %s)\n", wfID, taskID)
	fmt.Printf("Input passed is %s. Expected result: (5 + 10) * 2 = 30\n", string(inputData))

	// Give time for worker to process before exiting
	time.Sleep(3 * time.Second)
	fmt.Println("Example finished.")
}
