package sdk

import (
	"context"
	"log"
)

// Runtime is a consolidated interface combining client connection and local registry.
//
// Embedding the Client interface allows you to start workflows, wait for results,
// query executions, and close the underlying client connection directly on the runtime instance.
type Runtime interface {
	Client

	// Registry returns the local registry used for workflow step registration.
	Registry() Registry

	// NewWorker initializes a worker instance that automatically connects to the same
	// target server and uses the same step registry as this runtime.
	NewWorker(taskQueue string, workerID string) Worker

	// StartWorker instantiates a worker and starts it concurrently in a background goroutine.
	//
	// If the worker encounters an error during startup or execution, the onError callback
	// is executed. If onError is nil, errors are logged to standard output.
	StartWorker(ctx context.Context, taskQueue string, workerID string, onError func(err error)) Worker
}

type runtimeImpl struct {
	Client
	registry Registry
	target   string
}

// NewRuntime initializes a new AntFlow SDK Runtime.
//
// It establishes a connection to the AntFlow server at target (e.g. "localhost:50051")
// and initializes a clean workflow registry.
func NewRuntime(target string) (Runtime, error) {
	client, err := NewClient(target)
	if err != nil {
		return nil, err
	}

	return &runtimeImpl{
		Client:   client,
		registry: NewRegistry(),
		target:   target,
	}, nil
}

func (r *runtimeImpl) Registry() Registry {
	return r.registry
}

func (r *runtimeImpl) NewWorker(taskQueue string, workerID string) Worker {
	return NewWorker(WorkerOptions{
		Target:    r.target,
		TaskQueue: taskQueue,
		Registry:  r.registry,
		WorkerID:  workerID,
	})
}

func (r *runtimeImpl) StartWorker(ctx context.Context, taskQueue string, workerID string, onError func(err error)) Worker {
	w := r.NewWorker(taskQueue, workerID)
	go func() {
		if err := w.Start(ctx); err != nil {
			if onError != nil {
				onError(err)
			} else {
				log.Printf("Worker [%s] exited with error: %v", workerID, err)
			}
		}
	}()
	return w
}
