package sdk

import (
	"fmt"
)

// ChainWorkflowBuilder builds a CHAIN workflow.
type ChainWorkflowBuilder struct {
	name  string
	steps []workflowStep
}

// NewChainWorkflow creates a builder for a sequential (chained) workflow.
func NewChainWorkflow(name string) *ChainWorkflowBuilder {
	return &ChainWorkflowBuilder{
		name: name,
	}
}

// Step appends a sequential step to the chain.
func (b *ChainWorkflowBuilder) Step(name string, fn WorkflowFunc) *ChainWorkflowBuilder {
	b.steps = append(b.steps, workflowStep{name: name, fn: fn})
	return b
}

// Register registers the built CHAIN workflow with the provided local registry.
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

// NewIndependentWorkflow creates a builder for a concurrent (independent) workflow.
func NewIndependentWorkflow(name string) *IndependentWorkflowBuilder {
	return &IndependentWorkflowBuilder{
		name: name,
	}
}

// Step adds a standalone step to the execution list.
func (b *IndependentWorkflowBuilder) Step(name string, fn WorkflowFunc) *IndependentWorkflowBuilder {
	b.steps = append(b.steps, workflowStep{name: name, fn: fn})
	return b
}

// Register registers the built INDEPENDENT workflow with the provided local registry.
func (b *IndependentWorkflowBuilder) Register(r Registry) error {
	if impl, ok := r.(*registryImpl); ok {
		return impl.register(b.name, IndependentWorkflow, b.steps)
	}
	return fmt.Errorf("unsupported registry implementation")
}
