package sdk

import (
	"context"
	"fmt"
	"sync"
)

type WorkflowFunc func(ctx context.Context, input []byte) ([]byte, error)

type Registry interface {
	RegisterWorkflow(name string, wf WorkflowFunc)
	GetWorkflow(name string) (WorkflowFunc, error)
}

type registryImpl struct {
	mu        sync.RWMutex
	workflows map[string]WorkflowFunc
}

func NewRegistry() Registry {
	return &registryImpl{
		workflows: make(map[string]WorkflowFunc),
	}
}

func (r *registryImpl) RegisterWorkflow(name string, wf WorkflowFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workflows[name] = wf
}

func (r *registryImpl) GetWorkflow(name string) (WorkflowFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wf, ok := r.workflows[name]
	if !ok {
		return nil, fmt.Errorf("workflow '%s' not found in registry", name)
	}

	return wf, nil
}
