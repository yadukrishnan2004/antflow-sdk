package sdk

import (
	"context"
	"fmt"
	"io"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type App struct {
	target     string
	conn       *grpc.ClientConn
	grpcClient pb.WorkflowServiceClient
	registry   *registry
	pending    []*StepBuilder
}

type AppOption func(*App)

func WithServerAddress(target string) AppOption {
	return func(a *App) {
		a.target = target
	}
}

func NewApp(opts ...AppOption) (*App, error) {
	app := &App{
		target:   "localhost:50051",
		registry: newRegistry(),
	}

	for _, o := range opts {
		o(app)
	}

	conn, err := grpc.NewClient(app.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AntFlow server at %s: %w", app.target, err)
	}

	app.conn = conn
	app.grpcClient = pb.NewWorkflowServiceClient(conn)

	return app, nil
}

func (a *App) Workflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{
		name: name,
		app: a,
	}
}


func (a *App) NewWorker(taskQueue string, opts ...WorkerOption) Worker {
	return newWorker(a, taskQueue, opts...)
}

func (a *App) StartWorkflow(
	ctx context.Context,
	workflowName string,
	taskQueue string,
	input []byte,
)(string, error) {
	res, err := a.grpcClient.StartWorkflow(ctx, &pb.StartWorkflowRequest{
		WorkflowId: workflowName,
		Input: input,
		TaskQueue: taskQueue,
	})
	if err != nil {
		return "", fmt.Errorf("failed to start workflow '%s': %w", workflowName, err)
	}
	return res.Id, nil
}


func (a *App) track(sb *StepBuilder) {
	a.pending = append(a.pending, sb)
}

func (a *App) flushRegistrations() error {
	for _, sb := range a.pending {
		if err := sb.register(); err != nil {
			return fmt.Errorf("failed to register workflow '%s': %w", sb.name, err)
		}
	}
	a.pending = nil
	return nil
}


func (a *App) WaitForResult(ctx context.Context,workflowExecutionID string) ([]byte, error) {
	stream, err := a.grpcClient.StreamWorkflowHistory(ctx, &pb.StreamWorkflowHistoryRequest{
		WorkflowId: workflowExecutionID,
	})

	if err != nil {
		return nil , fmt.Errorf("failed to stream workflow history: %w", err)
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
            break
        }
        return nil, fmt.Errorf("history stream error: %w", err)
		}

		switch event.EventType {
		case "WORKFLOW_COMPLETED":
			return event.Result, nil
		case "WORKFLOW_FAILED", "WORKFLOW_CANCELLED":
			return nil, fmt.Errorf("workflow ended with status: %s", event.EventType)
		}
	}
	return a.getResult(ctx, workflowExecutionID)
}

func (a *App) CancelWorkflow(ctx context.Context, workflowExecutionID string) error {
	_, err := a.grpcClient.CancelWorkflow(ctx, &pb.CancelWorkflowRequest{
		WorkflowId: workflowExecutionID,
	})
	if err != nil {
		return fmt.Errorf("failed to cancel workflow: %w", err)
	}
	return nil
}

func (a *App) getResult(ctx context.Context, workflowExecutionID string) ([]byte, error) {
	res, err := a.grpcClient.GetWorkflowResult(ctx, &pb.GetWorkflowResultRequest{
		WorkflowId: workflowExecutionID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow result: %w", err)
	}
	if res.Error != "" {
		return nil, fmt.Errorf("workflow failed: %s", res.Error)
	}
	if res.State != "COMPLETED" {
		return nil, fmt.Errorf("workflow ended with status: %s", res.State)
	}
	return res.Result, nil
}

func (a *App) GetWorkflowResult(ctx context.Context, workflowExecutionID string) (string, []byte, error) {
	res, err := a.grpcClient.GetWorkflowResult(ctx, &pb.GetWorkflowResultRequest{
		WorkflowId: workflowExecutionID,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to get workflow result: %w", err)
	}
	if res.Error != "" {
		return res.State, nil, fmt.Errorf("workflow failed: %s", res.Error)
	}
	return res.State, res.Result, nil
}

func (a *App) Close() error {
	return a.conn.Close()
}