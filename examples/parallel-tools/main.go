package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Viking602/venat/tool"
)

type phaseDriver struct {
	name    string
	started chan<- string
	release <-chan struct{}
}

func (driver phaseDriver) Definition() tool.Definition {
	definition := tool.Definition{Name: driver.name, InputSchema: tool.Schema{Type: "object"}}
	if driver.name == "edit" {
		definition.Concurrency = tool.ConcurrencySequential
	}
	return definition
}

func (driver phaseDriver) Execute(_ context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	driver.started <- call.ID
	<-driver.release
	return tool.Result{ToolCallID: call.ID, Name: call.Name, Content: call.ID}, nil
}

func main() {
	started := make(chan string, 5)
	readsDone := make(chan struct{})
	editDone := make(chan struct{})
	writesDone := make(chan struct{})
	bus := tool.NewBus(
		phaseDriver{name: "read_a", started: started, release: readsDone},
		phaseDriver{name: "read_b", started: started, release: readsDone},
		phaseDriver{name: "edit", started: started, release: editDone},
		phaseDriver{name: "done_d", started: started, release: writesDone},
		phaseDriver{name: "done_e", started: started, release: writesDone},
	)
	calls := []tool.Call{
		{ID: "read-a", Name: "read_a", Arguments: json.RawMessage(`{}`)},
		{ID: "read-b", Name: "read_b", Arguments: json.RawMessage(`{}`)},
		{ID: "edit-call", Name: "edit", Arguments: json.RawMessage(`{}`)},
		{ID: "done-d", Name: "done_d", Arguments: json.RawMessage(`{}`)},
		{ID: "done-e", Name: "done_e", Arguments: json.RawMessage(`{}`)},
	}
	result := make(chan struct {
		results []tool.Result
		err     error
	}, 1)
	go func() {
		results, err := bus.ExecuteBatch(context.Background(), calls, tool.ModeParallel, tool.ExecuteOptions{})
		result <- struct {
			results []tool.Result
			err     error
		}{results, err}
	}()

	first, second := <-started, <-started
	if (first != "read-a" && first != "read-b") || (second != "read-a" && second != "read-b") || first == second {
		panic(fmt.Sprintf("reads did not overlap: %q, %q", first, second))
	}
	fmt.Println("read_a + read_b overlap")
	close(readsDone)
	if got := <-started; got != "edit-call" {
		panic(fmt.Sprintf("got %q before edit barrier", got))
	}
	fmt.Println("edit starts after both reads")
	close(editDone)
	postOne, postTwo := <-started, <-started
	if (postOne != "done-d" && postOne != "done-e") || (postTwo != "done-d" && postTwo != "done-e") || postOne == postTwo {
		panic(fmt.Sprintf("post-edit calls did not overlap: %q, %q", postOne, postTwo))
	}
	fmt.Println("done_d + done_e overlap after edit")
	close(writesDone)
	out := <-result
	if out.err != nil || len(out.results) != len(calls) {
		panic(fmt.Sprintf("batch failed: results=%d err=%v", len(out.results), out.err))
	}
}
