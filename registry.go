package sdk

import (
	"fmt"
	"sync"
)

type registry struct {
	mu        sync.RWMutex
	workflows map[string]*registeredWorkflow
}

func newRegistry() *registry {
	return &registry{
		workflows: make(map[string]*registeredWorkflow),
	}
}

func (r *registry) register(name string, wType WorkflowType, steps []workflowStep) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if name == "" {
		return fmt.Errorf("workflow name cannot be empty")
	}

	stepMap := make(map[string]WorkflowFunc)
	for _, step := range steps {
		if _, exists := stepMap[step.name]; exists {
			return fmt.Errorf("duplicate step '%s' in workflow '%s'", step.name, name)
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

func (r *registry) getStep(workflowName, stepName string) (WorkflowFunc, error) {
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

func (r *registry) getStepNames(workflowName string) ([]string, error) {
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

func (r *registry) getWorkflowType(workflowName string) (WorkflowType, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wf, ok := r.workflows[workflowName]
	if !ok {
		return "", fmt.Errorf("workflow '%s' not found in registry", workflowName)
	}

	return wf.workflowType, nil
}

func (r *registry) getAll() map[string]*registeredWorkflow {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m := make(map[string]*registeredWorkflow)
	for k, v := range r.workflows {
		m[k] = v
	}
	return m
}