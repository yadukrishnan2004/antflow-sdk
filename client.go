package sdk

import (
	"context"
	"fmt"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the command interface for controlling workflows through AntFlow.
//
// Use a client when your application needs to register workflows with the
// server, start executions, wait for results, inspect state, or cancel a running
// workflow.
type Client interface {
	// RegisterWorkflow registers a workflow name with the AntFlow server.
	//
	// Workers usually auto-register workflows on startup, but applications can
	// call this directly when they need explicit registration.
	RegisterWorkflow(ctx context.Context, name string) (string, error)

	// StartWorkflow starts a workflow execution on the given task queue.
	//
	// workflowName must match a registered workflow. input is passed to the first
	// workflow step and may contain any encoded payload, such as JSON bytes.
	StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (string, error)

	// GetWorkflowResult returns the current state and result for a workflow execution.
	//
	// If the workflow failed, the returned error includes the workflow failure
	// message from the server.
	GetWorkflowResult(ctx context.Context, workflowExecutionID string) (string, []byte, error)

	// CancelWorkflow requests cancellation for a running workflow execution.
	CancelWorkflow(ctx context.Context, workflowExecutionID string) error

	// WaitForResult blocks until the workflow completes, fails, is cancelled, or
	// the context is cancelled.
	//
	// On success it returns the final workflow result bytes.
	WaitForResult(ctx context.Context, workflowExecutionID string) ([]byte, error)

	// Close releases the underlying gRPC connection.
	Close() error
}

type clientImpl struct {
	conn       *grpc.ClientConn
	grpcClient pb.WorkflowServiceClient
}

// NewClient connects to an AntFlow server.
//
// target should be a gRPC address such as "localhost:50051".
func NewClient(target string) (Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	grpcClient := pb.NewWorkflowServiceClient(conn)
	return &clientImpl{
		conn:       conn,
		grpcClient: grpcClient,
	}, nil
}

func (c *clientImpl) RegisterWorkflow(ctx context.Context, name string) (string, error) {
	req := &pb.RegisterWorkflowRequest{
		Name: name,
	}

	res, err := c.grpcClient.RegisterWorkflow(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to register workflow via grpc: %w", err)
	}

	return res.Id, nil
}

func (c *clientImpl) StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (string, error) {
	req := &pb.StartWorkflowRequest{
		WorkflowId: workflowName,
		Input:      input,
		TaskQueue:  taskQueue,
	}

	res, err := c.grpcClient.StartWorkflow(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to start workflow via grpc: %w", err)
	}

	return res.WorkflowId, nil
}

func (c *clientImpl) Close() error {
	return c.conn.Close()
}

func (c *clientImpl) GetWorkflowResult(ctx context.Context, workflowExecutionID string) (string, []byte, error) {
	req := &pb.GetWorkflowResultRequest{
		WorkflowId: workflowExecutionID,
	}

	res, err := c.grpcClient.GetWorkflowResult(ctx, req)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get workflow result via grpc: %w", err)
	}

	if res.Error != "" {
		return res.State, nil, fmt.Errorf("workflow failed: %s", res.Error)
	}

	return res.State, res.Result, nil
}

func (c *clientImpl) CancelWorkflow(ctx context.Context, workflowExecutionID string) error {
	req := &pb.CancelWorkflowRequest{
		WorkflowId: workflowExecutionID,
	}

	_, err := c.grpcClient.CancelWorkflow(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to cancel workflow via grpc: %w", err)
	}

	return nil
}

func (c *clientImpl) WaitForResult(ctx context.Context, workflowExecutionID string) ([]byte, error) {
	req := &pb.StreamWorkflowHistoryRequest{
		WorkflowId: workflowExecutionID,
	}

	stream, err := c.grpcClient.StreamWorkflowHistory(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to start history stream: %w", err)
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			// Stream was closed gracefully on terminal event or error occurred.
			// Let's check GetWorkflowResult to get the final state if EOF
			break
		}

		if event.EventType == "WorkflowExecutionCompleted" {
			return event.Result, nil
		}
		if event.EventType == "WorkflowExecutionFailed" || event.EventType == "WorkflowExecutionCancelled" {
			return nil, fmt.Errorf("workflow ended in non-successful state: %s", event.EventType)
		}
	}

	// Fallback check if stream disconnected early
	state, result, err := c.GetWorkflowResult(ctx, workflowExecutionID)
	if err != nil {
		return nil, err
	}
	if state != "COMPLETED" {
		return nil, fmt.Errorf("workflow ended in non-successful state: %s", state)
	}
	return result, nil
}
