package sdk

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


const defaultPoolSize = 0

// Worker executes workflows from an AntFlow task queue.
type Worker interface {
	// Start connects to AntFlow, registers workflows, and begins processing tasks.
	// Blocks until ctx is cancelled or a stream error occurs.
	Start(ctx context.Context) error
}

type workerImpl struct {
	app *App
	taskQueue string
	workerID string
	poolSize int
}

// WorkerOption configures optional worker behaviour.
type WorkerOption func(*workerImpl)

func WithPoolSize(n int) WorkerOption {
	return func(w *workerImpl) {
		w.poolSize = n
	}
}

func newWorker(app *App, taskQueue string, opts ...WorkerOption) Worker {
	w := &workerImpl{
		app: app,
		taskQueue: taskQueue,
		workerID: generateWorkerID(),
		poolSize: defaultPoolSize,
	}

	for _, o := range opts {
		o(w)
	}
	if w.poolSize <= 0 {
		w.poolSize = runtime.NumCPU()
	}

	return w
}

func generateWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	pid := strconv.Itoa(os.Getpid())
	return fmt.Sprintf("%s-%s", host, pid)
}

func (w *workerImpl) Start(ctx context.Context) error {
	if err := w.app.flushRegistrations(); err != nil {
		return fmt.Errorf("worker [%s] failed to flush registrations: %w", w.workerID, err)
	}

	conn, err := grpc.NewClient(w.app.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("worker [%s] failed to connect: %w", w.workerID, err)
	}
	defer conn.Close()

	grpcClient := pb.NewWorkflowServiceClient(conn)

	log.Printf("Worker [%s] connected to %s, queue='%s', pool=%d",
		w.workerID, w.app.target, w.taskQueue, w.poolSize)

	if err := w.registerWorkflows(ctx, grpcClient); err != nil {
		return err
	}

	stream, err := grpcClient.StreamTasks(ctx, &pb.StreamTasksRequest{
		WorkerId:  w.workerID,
		TaskQueue: w.taskQueue,
	})
	if err != nil {
		return fmt.Errorf("worker [%s] failed to open task stream: %w", w.workerID, err)
	}

	compStream, err := grpcClient.StreamCompensationTasks(ctx, &pb.StreamTasksRequest{
		WorkerId:  w.workerID,
		TaskQueue: w.taskQueue,
	})
	if err != nil {
		return fmt.Errorf("worker [%s] failed to open compensation stream: %w", w.workerID, err)
	}

	taskCh := make(chan *pb.StreamTaskResponse, w.poolSize)
	compTaskCh := make(chan *pb.CompensationTaskResponse, w.poolSize)
	return w.runPool(ctx, grpcClient, stream, compStream, taskCh, compTaskCh)
}

func (w *workerImpl) registerWorkflows(ctx context.Context, grpcClient pb.WorkflowServiceClient) error {
	for name := range w.app.registry.getAll() {
		stepNames, _ := w.app.registry.getStepNames(name)
		compensationStepNames, _ := w.app.registry.getCompensationStepNames(name)
		wfType, _ := w.app.registry.getWorkflowType(name)

		_, err := grpcClient.RegisterWorkflow(ctx, &pb.RegisterWorkflowRequest{
			Name:              name,
			WorkflowType:      string(wfType),
			Steps:             stepNames,
			CompensationSteps: compensationStepNames,
		})
		if err != nil {
			log.Printf("Worker [%s] failed to register workflow '%s': %v", w.workerID, name, err)
		} else {
			log.Printf("Worker [%s] registered workflow '%s' (%s)", w.workerID, name, wfType)
		}
	}
	return nil
}

func (w *workerImpl) runPool(
	ctx context.Context,
	grpcClient pb.WorkflowServiceClient,
	stream pb.WorkflowService_StreamTasksClient,
	compStream pb.WorkflowService_StreamCompensationTasksClient,
	taskCh chan *pb.StreamTaskResponse,
	compTaskCh chan *pb.CompensationTaskResponse,
) error {
	var wg sync.WaitGroup

	// Spawn fixed pool of workers
	for i := range w.poolSize {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskCh:
					if !ok {
						return
					}
					w.processTask(ctx, grpcClient, task)
				case compTask, ok := <-compTaskCh:
					if !ok {
						return
					}
					w.processCompensationTask(ctx, grpcClient, compTask)
				}
			}
		}(i)
	}

	var streamErr error
	var once sync.Once
	closeChs := func() {
		once.Do(func() {
			close(taskCh)
			close(compTaskCh)
		})
	}

	// Reader for forward tasks
	go func() {
		for {
			task, err := stream.Recv()
			if err != nil {
				streamErr = fmt.Errorf("worker [%s] stream error: %w", w.workerID, err)
				closeChs()
				return
			}
			select {
			case <-ctx.Done():
				return
			case taskCh <- task:
			}
		}
	}()

	// Reader for compensation tasks
	go func() {
		for {
			compTask, err := compStream.Recv()
			if err != nil {
				if streamErr == nil {
					streamErr = fmt.Errorf("worker [%s] compensation stream error: %w", w.workerID, err)
				}
				closeChs()
				return
			}
			select {
			case <-ctx.Done():
				return
			case compTaskCh <- compTask:
			}
		}
	}()

	<-ctx.Done()
	closeChs()
	wg.Wait()
	return streamErr
}

func (w *workerImpl) processTask(ctx context.Context, grpcClient pb.WorkflowServiceClient, task *pb.StreamTaskResponse) {
	log.Printf("Worker [%s] processing task=%s workflow=%s step=%s",
		w.workerID, task.TaskId, task.WorkflowId, task.StepName)

	var result []byte
	var errString string

	stepFn, err := w.app.registry.getStep(task.Name, task.StepName)
	if err != nil {
		errString = err.Error()
		log.Printf("Worker [%s] task=%s step not found: %v", w.workerID, task.TaskId, err)
	} else {
		res, execErr := stepFn(ctx, task.Input)
		if execErr != nil {
			errString = execErr.Error()
			log.Printf("Worker [%s] task=%s step='%s' failed: %v", w.workerID, task.TaskId, task.StepName, execErr)
		} else {
			result = res
			log.Printf("Worker [%s] task=%s step='%s' completed", w.workerID, task.TaskId, task.StepName)
		}
	}

	_, completeErr := grpcClient.CompleteTask(ctx, &pb.CompleteTaskRequest{
		TaskId:   task.TaskId,
		WorkerId: w.workerID,
		Result:   result,
		Error:    errString,
	})

	if completeErr != nil {
		log.Printf("Worker [%s] failed to report task=%s completion: %v", w.workerID, task.TaskId, completeErr)
	}
}

func (w *workerImpl) processCompensationTask(ctx context.Context, grpcClient pb.WorkflowServiceClient, task *pb.CompensationTaskResponse) {
	log.Printf("Worker [%s] processing compensation task=%s workflow=%s step=%s compensation_step=%s",
		w.workerID, task.TaskId, task.WorkflowId, task.StepName, task.CompensationStepName)

	var result []byte
	var errString string

	stepFn, err := w.app.registry.getCompensationStep(task.Name, task.CompensationStepName)
	if err != nil {
		errString = err.Error()
		log.Printf("Worker [%s] compensation task=%s step not found: %v", w.workerID, task.TaskId, err)
	} else {
		res, execErr := stepFn(ctx, task.Input)
		if execErr != nil {
			errString = execErr.Error()
			log.Printf("Worker [%s] compensation task=%s step='%s' failed: %v", w.workerID, task.TaskId, task.CompensationStepName, execErr)
		} else {
			result = res
			log.Printf("Worker [%s] compensation task=%s step='%s' completed", w.workerID, task.TaskId, task.CompensationStepName)
		}
	}

	_, completeErr := grpcClient.CompleteCompensationTask(ctx, &pb.CompleteCompensationTaskRequest{
		TaskId:   task.TaskId,
		WorkerId: w.workerID,
		Result:   result,
		Error:    errString,
	})

	if completeErr != nil {
		log.Printf("Worker [%s] failed to report compensation task=%s completion: %v", w.workerID, task.TaskId, completeErr)
	}
}