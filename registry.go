package sdk

import (
	"context"
	"fmt"
	"sync"
)

type WorkflowFunc func(ctx context.Context, input []byte) ([]byte, error)

type WorkflowBuilder interface {
	AddStep(name string, wf WorkflowFunc) WorkflowBuilder
}

type Registry interface {
	RegisterWorkflow(name string) WorkflowBuilder
	GetWorkflow(name string) (WorkflowFunc, error)
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
