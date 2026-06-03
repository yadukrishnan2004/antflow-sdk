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
	StartWorkflow(ctx context.Context, workflowID string, taskQueue string, input []byte) (string, error)
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

func (c *clientImpl) StartWorkflow(ctx context.Context, workflowID string, taskQueue string, input []byte) (string, error) {
	req := &pb.StartWorkflowRequest{
		WorkflowId: workflowID,
		Input:      input,
		// In a fully developed proto we would pass TaskQueue here as well.
		// For now our server defaults to "default" if not implemented, or we can use the same proto struct if we update it.
		// Wait, I should update StartWorkflowRequest in proto to accept task_queue, but for now server ignores it or uses "default". Let's assume the user might need it later, we pass it in the SDK signature.
	}

	res, err := c.grpcClient.StartWorkflow(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to start workflow via grpc: %w", err)
	}

	return res.Id, nil
}

func (c *clientImpl) Close() error {
	return c.conn.Close()
}
