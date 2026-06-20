package sdk

import (
	"context"
)


type WorkflowFunc func(ctx context.Context, input []byte) ([]byte, error)

type WorkflowType string

const (
	ChainWorkflow       WorkflowType = "CHAIN"
	IndependentWorkflow WorkflowType = "INDEPENDENT"
	SagaWorkflow        WorkflowType = "SAGA"
)

type workflowStep struct {
	name             string
	fn               WorkflowFunc
	compensationName string
	compensationFn   WorkflowFunc
}

type registeredWorkflow struct {
	workflowType    WorkflowType
	steps           []workflowStep          
	stepMap         map[string]WorkflowFunc 
	compensationMap map[string]WorkflowFunc 
}
