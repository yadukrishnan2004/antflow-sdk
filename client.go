package sdk

import (
	"context"
	"fmt"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	RegisterWorkflow(ctx context.Context, name string) (string, error)
	StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (string, error)
	GetWorkflowResult(ctx context.Context, workflowExecutionID string) (string, []byte, error)
	CancelWorkflow(ctx context.Context, workflowExecutionID string) error
	WaitForResult(ctx context.Context, workflowExecutionID string) ([]byte, error)
	Close() error
}

type clientImpl struct {
	conn       *grpc.ClientConn
	grpcClient pb.WorkflowServiceClient
}

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
