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

type WorkflowType string

const (
	ChainWorkflow       WorkflowType = "CHAIN"
	IndependentWorkflow WorkflowType = "INDEPENDENT"
)

type workflowStep struct {
	name string
	fn   WorkflowFunc
}

// Internal stored definition
type registeredWorkflow struct {
	workflowType WorkflowType
	steps        []workflowStep          // ordered
	stepMap      map[string]WorkflowFunc // name → fn for O(1) lookup
}

type Registry interface {
	GetStep(workflowName, stepName string) (WorkflowFunc, error)
	GetStepNames(workflowName string) ([]string, error)
	GetWorkflowType(workflowName string) (WorkflowType, error)
	GetRegisteredWorkflows() map[string]*registeredWorkflow
}

type registryImpl struct {
	mu        sync.RWMutex
	workflows map[string]*registeredWorkflow
}

// NewRegistry creates an empty local workflow registry.
//
// Use the registry to define the workflows your worker knows how to execute:
//
//	registry := sdk.NewRegistry()
//	sdk.NewChainWorkflow("add").
//		Step("add", addWorkflow).
//		Register(registry)
//
// Pass the registry to a worker:
//
//	worker := sdk.NewWorker(sdk.WorkerOptions{
//		Target:    "localhost:50051",
//		TaskQueue: "calc-queue",
//		Registry:  registry,
//	})
func NewRegistry() Registry {
	return &registryImpl{
		workflows: make(map[string]*registeredWorkflow),
	}
}

func (r *registryImpl) register(name string, wType WorkflowType, steps []workflowStep) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if name == "" {
		return fmt.Errorf("workflow name cannot be empty")
	}

	stepMap := make(map[string]WorkflowFunc)
	for _, step := range steps {
		if _, exists := stepMap[step.name]; exists {
			return fmt.Errorf("step '%s' is registered multiple times in workflow '%s'", step.name, name)
		}
		stepMap[step.name] = step.fn
	}

	r.workflows[name] = &registeredWorkflow{
		workflowType: wType,
		steps:        steps,
		stepMap:      stepMap,
	}
	return nil
}

func (r *registryImpl) GetStep(workflowName, stepName string) (WorkflowFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wf, ok := r.workflows[workflowName]
	if !ok {
		return nil, fmt.Errorf("workflow '%s' not found in registry", workflowName)
	}

	fn, ok := wf.stepMap[stepName]
	if !ok {
		return nil, fmt.Errorf("step '%s' not found in workflow '%s'", stepName, workflowName)
	}

	return fn, nil
}

func (r *registryImpl) GetStepNames(workflowName string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wf, ok := r.workflows[workflowName]
	if !ok {
		return nil, fmt.Errorf("workflow '%s' not found in registry", workflowName)
	}

	names := make([]string, len(wf.steps))
	for i, step := range wf.steps {
		names[i] = step.name
	}
	return names, nil
}

func (r *registryImpl) GetWorkflowType(workflowName string) (WorkflowType, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wf, ok := r.workflows[workflowName]
	if !ok {
		return "", fmt.Errorf("workflow '%s' not found in registry", workflowName)
	}

	return wf.workflowType, nil
}

func (r *registryImpl) GetRegisteredWorkflows() map[string]*registeredWorkflow {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy of the map to prevent external modification issues
	m := make(map[string]*registeredWorkflow)
	for k, v := range r.workflows {
		m[k] = v
	}
	return m
}

// ChainWorkflowBuilder builds a CHAIN workflow.
type ChainWorkflowBuilder struct {
	name  string
	steps []workflowStep
}

func NewChainWorkflow(name string) *ChainWorkflowBuilder {
	return &ChainWorkflowBuilder{
		name: name,
	}
}

func (b *ChainWorkflowBuilder) Step(name string, fn WorkflowFunc) *ChainWorkflowBuilder {
	b.steps = append(b.steps, workflowStep{name: name, fn: fn})
	return b
}

func (b *ChainWorkflowBuilder) Register(r Registry) error {
	if impl, ok := r.(*registryImpl); ok {
		return impl.register(b.name, ChainWorkflow, b.steps)
	}
	return fmt.Errorf("unsupported registry implementation")
}

// IndependentWorkflowBuilder builds an INDEPENDENT workflow.
type IndependentWorkflowBuilder struct {
	name  string
	steps []workflowStep
}

func NewIndependentWorkflow(name string) *IndependentWorkflowBuilder {
	return &IndependentWorkflowBuilder{
		name: name,
	}
}

func (b *IndependentWorkflowBuilder) Step(name string, fn WorkflowFunc) *IndependentWorkflowBuilder {
	b.steps = append(b.steps, workflowStep{name: name, fn: fn})
	return b
}

func (b *IndependentWorkflowBuilder) Register(r Registry) error {
	if impl, ok := r.(*registryImpl); ok {
		return impl.register(b.name, IndependentWorkflow, b.steps)
	}
	return fmt.Errorf("unsupported registry implementation")
}
