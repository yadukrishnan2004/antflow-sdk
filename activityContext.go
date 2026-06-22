package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
)

var ErrSignalTimeout = errors.New("signal timeout")
 
// ActivityContext is passed to step functions that need to pause until an
// external signal arrives. It wraps a standard context.Context and adds
// signal-waiting capability backed by the server's SignalStore.
//
// Usage inside a step function:
//
//	func myStep(ctx context.Context, input []byte) ([]byte, error) {
//	    ac := sdk.ActivityContextFrom(ctx)
//	    payload, err := ac.WaitForSignal("payment-confirmed", 10*time.Minute)
//	    if errors.Is(err, sdk.ErrSignalTimeout) {
//	        return nil, fmt.Errorf("payment not confirmed within 10 minutes")
//	    }
//	    if err != nil {
//	        return nil, err
//	    }
//	    // payload is the bytes sent by the external caller via app.SendSignal(...)
//	    _ = payload
//	    return []byte("done"), nil
//	}
type ActivityContext struct {
	context.Context
 
	// ExecutionID is the workflow execution this step belongs to. The worker
	// sets this before calling the step function so WaitForSignal can route to
	// the correct server-side channel.
	ExecutionID string
 
	// grpcClient is the live connection to the AntFlow server, used to issue
	// PollSignal calls without requiring the step function to know about gRPC.
	grpcClient pb.WorkflowServiceClient
}
 
// activityContextKey is the unexported key used to store *ActivityContext in
// a context.Context so step functions can retrieve it with ActivityContextFrom.
type activityContextKey struct{}
 
// WithActivityContext returns a child context carrying ac.
func WithActivityContext(parent context.Context, ac *ActivityContext) context.Context {
	return context.WithValue(parent, activityContextKey{}, ac)
}
 
// ActivityContextFrom retrieves the *ActivityContext stored in ctx by
// WithActivityContext. Returns nil if ctx does not carry one — this happens
// when the step was invoked outside a worker (e.g. in a unit test), in which
// case the step should not call WaitForSignal.
func ActivityContextFrom(ctx context.Context) *ActivityContext {
	v, _ := ctx.Value(activityContextKey{}).(*ActivityContext)
	return v
}
 
// WaitForSignal blocks until the AntFlow server delivers the named signal to
// this execution, or until timeout elapses, or until ctx is cancelled.
//
// timeout == 0 means wait indefinitely (until ctx is cancelled).
//
// The returned byte slice is the payload passed to App.SendSignal by the
// external caller. The step function is responsible for unmarshalling it.
//
// Errors:
//   - ErrSignalTimeout  — timeout elapsed with no signal.
//   - ctx.Err()         — parent context cancelled (e.g. worker shutting down).
//   - any other error   — transient network / server error; the step should
//     return it so the worker can report it to CompleteTask and the server
//     will schedule a retry.
func (ac *ActivityContext) WaitForSignal(name string, timeout time.Duration) ([]byte, error) {
	if ac == nil {
		return nil, fmt.Errorf("WaitForSignal called outside an activity context; use sdk.ActivityContextFrom(ctx)")
	}
 
	var timeoutMs int64
	if timeout > 0 {
		timeoutMs = timeout.Milliseconds()
	}
 
	stream, err := ac.grpcClient.PollSignal(ac.Context, &pb.PollSignalRequest{
		ExecutionId: ac.ExecutionID,
		Name:        name,
		TimeoutMs:   timeoutMs,
	})
	if err != nil {
		return nil, fmt.Errorf("PollSignal: %w", err)
	}
 
	// The server sends exactly one SignalEvent then closes.
	event, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			// Stream closed without sending — treat as timeout.
			return nil, ErrSignalTimeout
		}
		return nil, fmt.Errorf("signal stream error: %w", err)
	}
 
	if event.TimedOut {
		return nil, ErrSignalTimeout
	}
 
	// Drain the rest of the stream (should already be closed, but be safe).
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}
 
	return event.Payload, nil
}