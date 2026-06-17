package sdk

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestIntegration_ChainWorkflow(t *testing.T) {
	// 1. Initialize Registry and register workflow steps
	registry := NewRegistry()
	err := NewChainWorkflow("integration-test-wf").
		Step("step-upper", func(ctx context.Context, input []byte) ([]byte, error) {
			return []byte(strings.ToUpper(string(input))), nil
		}).
		Step("step-reverse", func(ctx context.Context, input []byte) ([]byte, error) {
			runes := []rune(string(input))
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return []byte(string(runes)), nil
		}).
		Register(registry)
	if err != nil {
		t.Fatalf("failed to register workflow in registry: %v", err)
	}

	// 2. Start worker in a separate goroutine
	worker := NewWorker(WorkerOptions{
		Target:    "localhost:50051",
		TaskQueue: "integration-queue",
		Registry:  registry,
		WorkerID:  "integration-worker",
	})
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()

	go func() {
		if err := worker.Start(workerCtx); err != nil {
			log.Printf("worker stopped: %v", err)
		}
	}()

	// Give the worker a small window to connect and auto-register the workflow
	time.Sleep(1 * time.Second)

	// 3. Start client and trigger the workflow
	client, err := NewClient("localhost:50051")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	executionID, err := client.StartWorkflow(ctx, "integration-test-wf", "integration-queue", []byte("hello"))
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}
	t.Logf("started workflow execution with ID: %s", executionID)

	// 4. Wait for the final result
	result, err := client.WaitForResult(ctx, executionID)
	if err != nil {
		t.Fatalf("workflow failed: %v", err)
	}

	expected := "OLLEH" // upper("hello") = "HELLO", reverse("HELLO") = "OLLEH"
	if string(result) != expected {
		t.Fatalf("expected result %q, got %q", expected, string(result))
	}
	t.Logf("workflow completed successfully with result: %s", string(result))

	// 5. Query the database to verify the history events
	db, err := sql.Open("postgres", "postgres://postgres:1234@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT event_type 
		FROM history_event 
		WHERE workflow_execution_id = $1 
		ORDER BY id ASC
	`, executionID)
	if err != nil {
		t.Fatalf("failed to query history events: %v", err)
	}
	defer rows.Close()

	var eventTypes []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("failed to scan event type: %v", err)
		}
		eventTypes = append(eventTypes, et)
	}

	t.Logf("history events recorded: %v", eventTypes)

	// We expect:
	// - WORKFLOW_STARTED
	// - STEP_COMPLETED (for step-upper)
	// - STEP_SCHEDULED (for step-reverse)
	// - STEP_COMPLETED (for step-reverse)
	// - WORKFLOW_COMPLETED
	expectedEvents := []string{
		"WORKFLOW_STARTED",
		"STEP_COMPLETED",
		"STEP_SCHEDULED",
		"STEP_COMPLETED",
		"WORKFLOW_COMPLETED",
	}

	if len(eventTypes) != len(expectedEvents) {
		t.Errorf("expected %d events, got %d", len(expectedEvents), len(eventTypes))
	}

	for i, expectedName := range expectedEvents {
		if i < len(eventTypes) && eventTypes[i] != expectedName {
			t.Errorf("event at index %d: expected %q, got %q", i, expectedName, eventTypes[i])
		}
	}
}
