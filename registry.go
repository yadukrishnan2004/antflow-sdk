package sdk

import (
	"fmt"
	"sync"
)

// Registry contains the workflows this worker can execute.
type Registry interface {
	// GetStep resolves a specific step function by workflow name and step name.
	GetStep(workflowName, stepName string) (WorkflowFunc, error)

	// GetStepNames returns all step names defined for a workflow in registration order.
	GetStepNames(workflowName string) ([]string, error)

	// GetWorkflowType returns the registered pattern type of a workflow.
	GetWorkflowType(workflowName string) (WorkflowType, error)

	// GetRegisteredWorkflows retrieves a copy of all workflows currently registered.
	GetRegisteredWorkflows() map[string]*registeredWorkflow
}

type registryImpl struct {
	mu        sync.RWMutex
	workflows map[string]*registeredWorkflow
}

// NewRegistry creates an empty local workflow registry.
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

	m := make(map[string]*registeredWorkflow)
	for k, v := range r.workflows {
		m[k] = v
	}
	return m
}
