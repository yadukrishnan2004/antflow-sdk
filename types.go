package sdk

import (
	"context"
)

// WorkflowFunc is the function signature for a workflow step.
//
// Each step receives the current payload and returns the payload for the next
// step. Return an error to fail the workflow execution.
type WorkflowFunc func(ctx context.Context, input []byte) ([]byte, error)

// WorkflowType distinguishes the execution patterns supported by AntFlow.
type WorkflowType string

const (
	// ChainWorkflow executes steps sequentially: step1 -> step2 -> step3.
	ChainWorkflow WorkflowType = "CHAIN"

	// IndependentWorkflow executes steps concurrently as individual standalone tasks.
	IndependentWorkflow WorkflowType = "INDEPENDENT"
)

type workflowStep struct {
	name string
	fn   WorkflowFunc
}

// registeredWorkflow represents the stored internal workflow definition.
type registeredWorkflow struct {
	workflowType WorkflowType
	steps        []workflowStep          // ordered list of steps
	stepMap      map[string]WorkflowFunc // fast lookup map (stepName -> function)
}
