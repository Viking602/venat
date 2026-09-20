package durable

import (
	"slices"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
)

func cloneExecution(execution Execution) Execution {
	execution.Spec = cloneExecutionSpec(execution.Spec)
	if execution.Lease != nil {
		lease := *execution.Lease
		execution.Lease = &lease
	}
	if execution.Checkpoint != nil {
		checkpoint := *execution.Checkpoint
		checkpoint.Continuation = cloneContinuation(execution.Checkpoint.Continuation)
		execution.Checkpoint = &checkpoint
	}
	if execution.Result != nil {
		result := cloneAgentResult(*execution.Result)
		execution.Result = &result
	}
	return execution
}

func cloneExecutionSpec(spec ExecutionSpec) ExecutionSpec {
	spec.Request = cloneRequest(spec.Request)
	spec.OutputPolicy = cloneOutputPolicy(spec.OutputPolicy)
	return spec
}

func cloneRequest(request agent.Request) agent.Request {
	request.Content = message.CloneContent(request.Content)
	if request.Budget != nil {
		budget := *request.Budget
		request.Budget = &budget
	}
	if request.SessionBudget != nil {
		budget := *request.SessionBudget
		request.SessionBudget = &budget
	}
	if request.ModelTimeouts != nil {
		policy := *request.ModelTimeouts
		request.ModelTimeouts = &policy
	}
	return request
}

func cloneOutputPolicy(policy agent.OutputPolicy) agent.OutputPolicy {
	policy.Schema = slices.Clone(policy.Schema)
	return policy
}

func cloneContinuation(continuation agent.Continuation) agent.Continuation {
	continuation.Request = cloneRequest(continuation.Request)
	continuation.OutputPolicy = cloneOutputPolicy(continuation.OutputPolicy)
	continuation.Messages = message.CloneMessages(continuation.Messages)
	continuation.Steps = cloneSteps(continuation.Steps)
	return continuation
}

func cloneSteps(steps []agent.Step) []agent.Step {
	if steps == nil {
		return nil
	}
	cloned := slices.Clone(steps)
	for index := range cloned {
		if steps[index].ModelCall != nil {
			modelCall := *steps[index].ModelCall
			cloned[index].ModelCall = &modelCall
		}
		cloned[index].ToolCalls = slices.Clone(steps[index].ToolCalls)
		for callIndex := range cloned[index].ToolCalls {
			cloned[index].ToolCalls[callIndex].Arguments = slices.Clone(steps[index].ToolCalls[callIndex].Arguments)
			cloned[index].ToolCalls[callIndex].Output = slices.Clone(steps[index].ToolCalls[callIndex].Output)
		}
		cloned[index].Observations = slices.Clone(steps[index].Observations)
	}
	return cloned
}

func cloneAttempts(attempts []Attempt) []Attempt {
	if attempts == nil {
		return nil
	}
	cloned := slices.Clone(attempts)
	for index := range cloned {
		cloned[index].Payload = slices.Clone(attempts[index].Payload)
		if attempts[index].Lease != nil {
			lease := *attempts[index].Lease
			cloned[index].Lease = &lease
		}
		cloned[index].Failure = cloneFailureRecord(attempts[index].Failure)
	}
	return cloned
}

func cloneAgentResult(result agent.Result) agent.Result {
	result.Structured = slices.Clone(result.Structured)
	if result.Failure != nil {
		failure := *result.Failure
		result.Failure = &failure
	}
	result.Steps = cloneSteps(result.Steps)
	result.Messages = message.CloneMessages(result.Messages)
	return result
}
