package sdk

import (
	"context"
	"fmt"
	"sync"
)

// WorkflowFunc is the function signature for a workflow step.
//
// Each step receives the current payload and returns the payload for the next
// step. Return an error to fail the workflow execution.
type WorkflowFunc func(ctx context.Context, input []byte) ([]byte, error)

// WorkflowBuilder defines a workflow by chaining one or more executable steps.
type WorkflowBuilder interface {
	// AddStep appends a named step to the workflow.
	//
	// Steps run in the order they are added. The output from one step becomes
	// the input for the next step.
	AddStep(name string, wf WorkflowFunc) WorkflowBuilder
}

// Registry stores local workflow definitions for workers.
//
// Create one registry per worker process, register each workflow your worker can
// execute, then pass it to NewWorker through WorkerOptions.
type Registry interface {
	// RegisterWorkflow creates or replaces a workflow definition with the given name.
	//
	// The returned builder lets you attach the workflow steps:
	//
	//	registry.RegisterWorkflow("resize-image").
	//		AddStep("download", downloadImage).
	//		AddStep("resize", resizeImage).
	//		AddStep("upload", uploadImage)
	RegisterWorkflow(name string) WorkflowBuilder

	// GetWorkflow returns an executable workflow function by name.
	//
	// Workers use this internally when a task arrives from the server.
	GetWorkflow(name string) (WorkflowFunc, error)

	// GetRegisteredNames returns the names of all workflows in this registry.
	//
	// Workers use this list to auto-register available workflows with the server
	// when they start.
	GetRegisteredNames() []string
}

type workflowStep struct {
	name string
	fn   WorkflowFunc
}

type workflowBuilderImpl struct {
	steps []workflowStep
}

func (b *workflowBuilderImpl) AddStep(name string, wf WorkflowFunc) WorkflowBuilder {
	b.steps = append(b.steps, workflowStep{name: name, fn: wf})
	return b
}

type registryImpl struct {
	mu        sync.RWMutex
	workflows map[string]*workflowBuilderImpl
}

// NewRegistry creates an empty local workflow registry.
//
// Use the registry to define the workflows your worker knows how to execute:
//
//	registry := sdk.NewRegistry()
//	registry.RegisterWorkflow("add").
//		AddStep("add", addWorkflow)
//
// Pass the registry to a worker:
//
//	worker := sdk.NewWorker(sdk.WorkerOptions{
//		Target:    "localhost:50051",
//		TaskQueue: "calc-queue",
//		Registry:  registry,
//	})
//
// For remote workflow commands, create a client with NewClient and use:
//
//	client.RegisterWorkflow(ctx, name)
//	client.StartWorkflow(ctx, workflowName, taskQueue, input)
//	client.GetWorkflowResult(ctx, workflowExecutionID)
//	client.CancelWorkflow(ctx, workflowExecutionID)
//	client.WaitForResult(ctx, workflowExecutionID)
//	client.Close()
func NewRegistry() Registry {
	return &registryImpl{
		workflows: make(map[string]*workflowBuilderImpl),
	}
}

func (r *registryImpl) RegisterWorkflow(name string) WorkflowBuilder {
	r.mu.Lock()
	defer r.mu.Unlock()
	builder := &workflowBuilderImpl{}
	r.workflows[name] = builder
	return builder
}

func (r *registryImpl) GetRegisteredNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var names []string
	for name := range r.workflows {
		names = append(names, name)
	}
	return names
}

func (r *registryImpl) GetWorkflow(name string) (WorkflowFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	builder, ok := r.workflows[name]
	if !ok {
		return nil, fmt.Errorf("workflow '%s' not found in registry", name)
	}

	// Create a wrapper function that executes all steps sequentially
	return func(ctx context.Context, input []byte) ([]byte, error) {
		var currentInput []byte = input
		var err error

		for _, step := range builder.steps {
			currentInput, err = step.fn(ctx, currentInput)
			if err != nil {
				return nil, fmt.Errorf("step '%s' failed: %w", step.name, err)
			}
		}

		return currentInput, nil
	}, nil
}
