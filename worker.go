package sdk

import (
	"context"
	"fmt"
	"log"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Worker executes workflows from an AntFlow task queue.
//
// A worker connects to the AntFlow server, advertises the workflows available in
// its Registry, listens for tasks on a task queue, and reports completed results
// back to the server.
type Worker interface {
	// Start connects the worker to AntFlow and begins processing tasks.
	//
	// Start blocks until the context is cancelled or the worker encounters a
	// stream error.
	Start(ctx context.Context) error
}

type workerImpl struct {
	target    string
	taskQueue string
	registry  Registry
	workerID  string
}

// WorkerOptions configures a worker process.
type WorkerOptions struct {
	// Target is the AntFlow server gRPC address, such as "localhost:50051".
	Target string

	// TaskQueue is the queue this worker will poll for workflow tasks.
	//
	// StartWorkflow must use the same task queue for this worker to receive the
	// execution.
	TaskQueue string

	// Registry contains the workflows this worker can execute.
	//
	// Register workflows with NewRegistry before passing the registry to NewWorker.
	Registry Registry

	// WorkerID is an optional stable identifier for logs and server coordination.
	//
	// If empty, NewWorker uses "default-worker".
	WorkerID string
}

// NewWorker creates a worker for the provided task queue and registry.
//
// Example:
//
//	registry := sdk.NewRegistry()
//	registry.RegisterWorkflow("add").AddStep("add", addWorkflow)
//
//	worker := sdk.NewWorker(sdk.WorkerOptions{
//		Target:    "localhost:50051",
//		TaskQueue: "calc-queue",
//		Registry:  registry,
//		WorkerID:  "calc-worker-1",
//	})
//
//	err := worker.Start(ctx)
func NewWorker(opts WorkerOptions) Worker {
	if opts.WorkerID == "" {
		opts.WorkerID = "default-worker"
	}
	return &workerImpl{
		target:    opts.Target,
		taskQueue: opts.TaskQueue,
		registry:  opts.Registry,
		workerID:  opts.WorkerID,
	}
}

func (w *workerImpl) Start(ctx context.Context) error {
	conn, err := grpc.NewClient(w.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer conn.Close()

	grpcClient := pb.NewWorkflowServiceClient(conn)

	log.Printf("Worker [%s] connected to %s, listening on queue '%s'", w.workerID, w.target, w.taskQueue)

	// Auto-register workflows with the server
	registeredNames := w.registry.GetRegisteredNames()
	for _, name := range registeredNames {
		_, err := grpcClient.RegisterWorkflow(ctx, &pb.RegisterWorkflowRequest{Name: name})
		if err != nil {
			log.Printf("Worker [%s] failed to register workflow '%s': %v", w.workerID, name, err)
		} else {
			log.Printf("Worker [%s] successfully registered workflow '%s' with server", w.workerID, name)
		}
	}

	// Open the stream
	stream, err := grpcClient.StreamTasks(ctx, &pb.StreamTasksRequest{
		WorkerId:  w.workerID,
		TaskQueue: w.taskQueue,
	})
	if err != nil {
		return fmt.Errorf("failed to open task stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker context cancelled, stopping...")
			return nil
		default:
			// Recv blocks until a task is available (or stream errors out)
			taskResp, err := stream.Recv()
			if err != nil {
				// Stream closed or error, handle reconnection logic in a real system
				return fmt.Errorf("stream read error: %w", err)
			}

			w.processTask(ctx, grpcClient, taskResp)
		}
	}
}

func (w *workerImpl) processTask(ctx context.Context, client pb.WorkflowServiceClient, taskResp *pb.StreamTaskResponse) {
	log.Printf("Worker [%s] picked up task %s for workflow %s", w.workerID, taskResp.TaskId, taskResp.WorkflowId)

	var result []byte
	var errString string

	wfFunc, err := w.registry.GetWorkflow(taskResp.Name)
	if err != nil {
		errString = err.Error()
		log.Printf("Task %s failed: %v", taskResp.TaskId, err)
	} else {
		// Execute the workflow logic
		res, execErr := wfFunc(ctx, taskResp.Input)
		if execErr != nil {
			errString = execErr.Error()
			log.Printf("Task %s failed during execution: %v", taskResp.TaskId, execErr)
		} else {
			result = res
			log.Printf("Task %s completed successfully", taskResp.TaskId)
		}
	}

	// Complete the task
	_, completeErr := client.CompleteTask(ctx, &pb.CompleteTaskRequest{
		TaskId:   taskResp.TaskId,
		WorkerId: w.workerID,
		Result:   result,
		Error:    errString,
	})

	if completeErr != nil {
		log.Printf("Failed to report completion for task %s: %v", taskResp.TaskId, completeErr)
	}
}
