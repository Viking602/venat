package orchestration

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/tool"
)

// Steer redirects a running worker without canceling its whole lifetime. Active
// model sampling is interrupted where safe; already started tool effects finish.
// For an idle worker it schedules a history-preserving follow-up turn. Running
// workers acknowledge consumption, while idle workers acknowledge queue admission.
func (runtime *Runtime) Steer(ctx context.Context, id string, request agent.Request) error {
	if runtime == nil {
		return ErrRuntimeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return fmt.Errorf("%w: steering: %v", ErrInvalidArgument, err)
	}
	runtime.mu.RLock()
	state := runtime.tasks[id]
	runtime.mu.RUnlock()
	if state == nil {
		return ErrTaskNotFound
	}
	state.mu.Lock()
	if state.status == TaskCanceled {
		state.mu.Unlock()
		return context.Canceled
	}
	if state.status == TaskIdle {
		state.mu.Unlock()
		return runtime.SendFollowup(ctx, id, request)
	}
	if state.status != TaskRunning || state.control == nil {
		state.mu.Unlock()
		return ErrTaskBusy
	}
	ack, err := state.control.Steer(request)
	state.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Steer changes this worker's direction while keeping its history and identity.
func (handle TaskHandle) Steer(ctx context.Context, request agent.Request) error {
	if handle.runtime == nil {
		return ErrTaskNotFound
	}
	return handle.runtime.Steer(ctx, handle.id, request)
}

func (driver collaborationTool) executeSteer(ctx context.Context, call tool.Call) (tool.Result, error) {
	var input struct {
		ID     string `json:"id"`
		Prompt string `json:"prompt"`
	}
	if err := json.Unmarshal(call.Arguments, &input); err != nil {
		return collaborationToolError(call, err), nil
	}
	if err := driver.runtime.Steer(ctx, input.ID, agent.Request{Prompt: input.Prompt}); err != nil {
		return collaborationToolError(call, err), nil
	}
	return collaborationToolOutput(call, map[string]string{"taskId": input.ID, "status": "direction accepted"}), nil
}
