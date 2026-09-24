package tool

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/Viking602/venat/message"
)

type (
	Definition      = message.ToolDefinition
	Schema          = message.JSONSchema
	Call            = message.ToolCall
	Result          = message.ToolResult
	ConcurrencyMode = message.ToolConcurrencyMode
)

const (
	DefaultBatchConcurrency = 32
	MaxBatchCalls           = 1024
)

// ErrNotExecuted marks a tool error that occurred before the underlying
// operation started. Durable action recovery may safely record it as failed.
var ErrNotExecuted = errors.New("tool operation was not executed")

// ErrToolTimeout is the cause of a tool-local deadline, distinct from cancellation
// of its caller. Only a driver with confirmed termination may turn it into feedback.
var ErrToolTimeout = errors.New("tool-local deadline exceeded")

const (
	ConcurrencyParallel   = message.ToolConcurrencyParallel
	ConcurrencySequential = message.ToolConcurrencySequential
	ConcurrencyExclusive  = message.ToolConcurrencyExclusive
)

type Mode string

const (
	ModeSequential Mode = "sequential"
	ModeParallel   Mode = "parallel"
)

// UpdateKind classifies one transient tool callback value.
type UpdateKind string

const (
	// UpdateProgress reports message/data state and must not contain Parts.
	UpdateProgress UpdateKind = "progress"
	// UpdateOutput reports ordered output and must contain at least one Part.
	UpdateOutput UpdateKind = "output"
)

// Update is one transient, per-call tool update. Bus overwrites identity and
// sequence fields before delivery.
type Update struct {
	Kind        UpdateKind            `json:"kind"`
	ToolCallID  string                `json:"toolCallId,omitempty"`
	OperationID string                `json:"operationId,omitempty"`
	Sequence    uint64                `json:"sequence"`
	Message     string                `json:"message,omitempty"`
	Data        map[string]string     `json:"data,omitempty"`
	Parts       []message.ContentPart `json:"parts,omitempty"`
}

// UpdateSink synchronously receives one update and applies backpressure by
// returning only when the producer may continue.
type UpdateSink func(Update) error

// ExecuteOptions configures one Bus dispatch.
type ExecuteOptions struct {
	Sink        UpdateSink
	Interceptor Interceptor
}

type Driver interface {
	Definition() Definition
	Execute(ctx context.Context, call Call, sink UpdateSink) (Result, error)
}

var ErrToolNotFound = errors.New("tool not found")

// ErrToolPanic wraps a panic recovered from a tool driver running on a
// goroutine the bus spawns for parallel execution. Such a panic cannot be
// recovered by the caller's stack, so executeParallel recovers it in the
// worker goroutine and records it as that call's error — surfaced by
// errors.Join exactly like any other tool failure — rather than letting it
// unwind the runtime and crash the process. In sequential mode a driver runs
// inline on the caller's stack, so its panic propagates to the caller's own
// recover instead. errors.Is(err, ErrToolPanic) reports whether a batch error
// originated from a panicking driver.
var ErrToolPanic = errors.New("tool driver panicked")

var (
	ErrDuplicateToolName     = errors.New("duplicate tool name")
	ErrInvalidToolDefinition = errors.New("invalid tool definition")
	ErrTooManyToolCalls      = errors.New("tool batch exceeds safe call limit")
)

// CallExecutionError attributes one batch failure to its durable tool-call
// slot. ErrNotExecuted in its chain proves the driver never started.
type CallExecutionError struct {
	CallID string
	Err    error
}

func (failure CallExecutionError) Error() string {
	return fmt.Sprintf("tool call %s: %v", failure.CallID, failure.Err)
}

func (failure CallExecutionError) Unwrap() error { return failure.Err }

// BatchExecutionError preserves every per-call failure from one dispatch.
type BatchExecutionError struct {
	Failures []CallExecutionError
}

func (failure *BatchExecutionError) Error() string {
	return fmt.Sprintf("%d tool call(s) failed", len(failure.Failures))
}

func (failure *BatchExecutionError) Unwrap() []error {
	errors := make([]error, len(failure.Failures))
	for index := range failure.Failures {
		errors[index] = failure.Failures[index]
	}
	return errors
}

// NotExecutedCallIDs returns the call slots proven not to have started and
// whether every batch failure has that proof.
func NotExecutedCallIDs(err error) (map[string]struct{}, bool) {
	var batch *BatchExecutionError
	if !errors.As(err, &batch) || len(batch.Failures) == 0 {
		return nil, false
	}
	ids := make(map[string]struct{})
	all := true
	for _, failure := range batch.Failures {
		if errors.Is(failure.Err, ErrNotExecuted) {
			ids[failure.CallID] = struct{}{}
		} else {
			all = false
		}
	}
	return ids, all
}

type concurrencyLimiter struct {
	capacity int
	permits  chan struct{}
}

func (limiter *concurrencyLimiter) acquire(ctx context.Context) (func(), error) {
	if limiter == nil {
		return func() {}, nil
	}
	select {
	case limiter.permits <- struct{}{}:
		return func() { <-limiter.permits }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type Bus struct {
	mu          sync.RWMutex
	drivers     map[string]Driver
	definitions map[string]Definition
	validations map[string]argumentValidation
	limiters    map[string]*concurrencyLimiter
	err         error
}

func NewBus(drivers ...Driver) *Bus {
	bus := &Bus{
		drivers:     make(map[string]Driver, len(drivers)),
		definitions: make(map[string]Definition, len(drivers)),
		validations: make(map[string]argumentValidation, len(drivers)),
		limiters:    make(map[string]*concurrencyLimiter),
	}
	for _, driver := range drivers {
		if err := bus.Register(driver); err != nil {
			bus.mu.Lock()
			bus.err = errors.Join(bus.err, err)
			bus.mu.Unlock()
		}
	}
	return bus
}

func (b *Bus) Register(driver Driver) error {
	if driver == nil {
		return fmt.Errorf("%w: driver is nil", ErrInvalidToolDefinition)
	}
	definition := driver.Definition()
	if definition.Name == "" {
		return fmt.Errorf("%w: name is empty", ErrInvalidToolDefinition)
	}
	validation := compileArgumentValidation(definition)
	if validation.err != nil {
		return validation.err
	}
	limiterKey, limiterCapacity, err := concurrencySpec(definition)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.drivers[definition.Name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateToolName, definition.Name)
	}
	if limiterKey != "" {
		if existing := b.limiters[limiterKey]; existing != nil && existing.capacity != limiterCapacity {
			return fmt.Errorf("%w: concurrency group %q has limits %d and %d", ErrInvalidToolDefinition, limiterKey, existing.capacity, limiterCapacity)
		}
		if b.limiters[limiterKey] == nil {
			b.limiters[limiterKey] = &concurrencyLimiter{capacity: limiterCapacity, permits: make(chan struct{}, limiterCapacity)}
		}
	}
	b.drivers[definition.Name] = driver
	b.definitions[definition.Name] = cloneDefinition(definition)
	b.validations[definition.Name] = validation
	return nil
}

func concurrencySpec(definition Definition) (string, int, error) {
	mode := definition.Concurrency
	if mode == "" {
		mode = ConcurrencyParallel
	}
	capacity := definition.MaxConcurrency
	if capacity < 0 {
		return "", 0, fmt.Errorf("%w: %s has negative max concurrency", ErrInvalidToolDefinition, definition.Name)
	}
	switch mode {
	case ConcurrencyParallel:
	case ConcurrencySequential, ConcurrencyExclusive:
		if capacity > 1 {
			return "", 0, fmt.Errorf("%w: %s mode %s requires max concurrency 0 or 1", ErrInvalidToolDefinition, definition.Name, mode)
		}
		capacity = 1
	default:
		return "", 0, fmt.Errorf("%w: %s has concurrency mode %q", ErrInvalidToolDefinition, definition.Name, mode)
	}
	if capacity == 0 {
		return "", 0, nil
	}
	group := strings.TrimSpace(definition.ConcurrencyGroup)
	if group == "" {
		group = definition.Name
	}
	return group, capacity, nil
}

// Validate reports construction-time registration and schema failures.
func (b *Bus) Validate() error {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.err
}

func (b *Bus) Definitions() []Definition {
	b.mu.RLock()
	defer b.mu.RUnlock()
	defs := make([]Definition, 0, len(b.definitions))
	for _, definition := range b.definitions {
		defs = append(defs, cloneDefinition(definition))
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	return defs
}

func (b *Bus) IsTerminal(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.definitions[name].Terminal
}

// Clone returns an independently registerable bus that shares the immutable
// registered drivers, validation plans, and concurrency limiters. Sharing
// limiters preserves exclusive and bounded policies across agent runs.
func (b *Bus) Clone() *Bus {
	if b == nil {
		return NewBus()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	cloned := &Bus{
		drivers:     make(map[string]Driver, len(b.drivers)),
		definitions: make(map[string]Definition, len(b.definitions)),
		validations: make(map[string]argumentValidation, len(b.validations)),
		limiters:    make(map[string]*concurrencyLimiter, len(b.limiters)),
		err:         b.err,
	}
	for name, driver := range b.drivers {
		cloned.drivers[name] = driver
		cloned.definitions[name] = cloneDefinition(b.definitions[name])
		cloned.validations[name] = b.validations[name]
	}
	maps.Copy(cloned.limiters, b.limiters)
	return cloned
}

// MapDrivers returns a policy-preserving bus whose drivers are wrapped by
// mapper. Definitions, validation plans, and shared concurrency limiters remain
// those frozen by the source bus.
func (b *Bus) MapDrivers(mapper func(Definition, Driver) Driver) *Bus {
	cloned := b.Clone()
	if b == nil || mapper == nil {
		return cloned
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for name, driver := range b.drivers {
		mapped := mapper(cloneDefinition(b.definitions[name]), driver)
		if mapped == nil {
			cloned.err = errors.Join(cloned.err, fmt.Errorf("%w: mapped driver %s is nil", ErrInvalidToolDefinition, name))
			continue
		}
		if mapped.Definition().Name != name {
			cloned.err = errors.Join(cloned.err, fmt.Errorf(
				"%w: mapped driver renamed %s to %s",
				ErrInvalidToolDefinition,
				name,
				mapped.Definition().Name,
			))
			continue
		}
		cloned.drivers[name] = mapped
	}
	return cloned
}

func (b *Bus) Subset(names []string) *Bus {
	if b == nil || len(names) == 0 {
		return NewBus()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	subset := &Bus{
		drivers:     make(map[string]Driver, len(names)),
		definitions: make(map[string]Definition, len(names)),
		validations: make(map[string]argumentValidation, len(names)),
		limiters:    make(map[string]*concurrencyLimiter, len(b.limiters)),
		err:         b.err,
	}
	maps.Copy(subset.limiters, b.limiters)
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			subset.err = errors.Join(subset.err, fmt.Errorf("%w: %s", ErrDuplicateToolName, name))
			continue
		}
		seen[name] = struct{}{}
		if driver, ok := b.drivers[name]; ok {
			subset.drivers[name] = driver
			subset.definitions[name] = cloneDefinition(b.definitions[name])
			subset.validations[name] = b.validations[name]
		}
	}
	return subset
}

func (b *Bus) Driver(name string) (Driver, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	driver, ok := b.drivers[name]
	return driver, ok
}

// Execute returns unknown names and invalid arguments as completed IsError
// results. Driver/infrastructure errors remain Go errors and stop a batch.
func (b *Bus) Execute(ctx context.Context, call Call, options ExecuteOptions) (Result, error) {
	if err := b.Validate(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, errors.Join(ErrNotExecuted, err)
	}
	b.mu.RLock()
	driver, ok := b.drivers[call.Name]
	validation := b.validations[call.Name]
	definition := b.definitions[call.Name]
	key, _, _ := concurrencySpec(definition)
	limiter := b.limiters[key]
	b.mu.RUnlock()
	if !ok {
		return rejectedCall(call, fmt.Errorf("%w: %s; choose an available tool (%s)", ErrToolNotFound, call.Name, b.availableToolsHint(call.Name))), nil
	}
	if validation.err != nil {
		return Result{}, validation.err
	}
	if err := validation.validate(call.Arguments); err != nil {
		return rejectedCall(call, err), nil
	}
	definition = cloneDefinition(definition)
	if definition.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, definition.Timeout, ErrToolTimeout)
		defer cancel()
	}
	release, err := limiter.acquire(ctx)
	if err != nil {
		return Result{}, errors.Join(ErrNotExecuted, err)
	}
	defer release()
	terminal := toolUpdateDriver{next: driver}
	interceptor := ChainInterceptors(options.Interceptor)
	if interceptor == nil {
		return terminal.Execute(ctx, cloneCall(call), options.Sink)
	}
	return interceptor.Execute(ctx, terminal, call, options.Sink)
}

// availableToolsHint builds the unknown-name rejection hint. Candidates are
// ranked by nameSimilarity so the tool the model likely meant surfaces first,
// and strong matches are called out as a "did you mean" suggestion. Only
// dispatchable names are disclosed: restricted or unregistered tools are never
// named because the model cannot reach them on this bus.
func (b *Bus) availableToolsHint(query string) string {
	b.mu.RLock()
	candidates := make([]Definition, 0, len(b.definitions))
	for _, definition := range b.definitions {
		candidates = append(candidates, definition)
	}
	b.mu.RUnlock()
	if len(candidates) == 0 {
		return "no tools are registered"
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := nameSimilarity(query, candidates[i]), nameSimilarity(query, candidates[j])
		if left != right {
			return left > right
		}
		return candidates[i].Name < candidates[j].Name
	})
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		names = append(names, candidate.Name)
	}
	const maxNamed = 12
	list := names
	if len(list) > maxNamed {
		list = list[:maxNamed]
	}
	hint := "available: " + strings.Join(list, ", ")
	if len(names) > maxNamed {
		hint += fmt.Sprintf(", and %d more", len(names)-maxNamed)
	}
	if top := nameSimilarity(query, candidates[0]); top >= didYouMeanThreshold {
		suggested := make([]string, 0, maxSuggestions)
		for _, candidate := range candidates {
			if len(suggested) == maxSuggestions || nameSimilarity(query, candidate) < top-0.05 {
				break
			}
			suggested = append(suggested, candidate.Name)
		}
		hint = fmt.Sprintf("did you mean: %s? %s", strings.Join(suggested, ", "), hint)
	}
	return hint
}

const (
	didYouMeanThreshold = 0.3
	maxSuggestions      = 3
)

// nameSimilarity scores how likely definition is the tool a rejected call
// meant to reach. It is a deterministic heuristic over names and descriptions,
// never a rewrite decision. Containment catches vendor-prior names with
// affixes (agent_update_plan -> update_plan), edit distance catches typos and
// renames, and token overlap with the description catches training vocabulary
// the model still reaches for (apply_patch -> edit_file whose description
// names that tool family).
func nameSimilarity(query string, definition Definition) float64 {
	normalizedQuery, normalizedName := normalizeName(query), normalizeName(definition.Name)
	if normalizedQuery == "" || normalizedName == "" {
		return 0
	}
	longest := max(len(normalizedQuery), len(normalizedName))
	score := 1 - float64(levenshtein(normalizedQuery, normalizedName))/float64(longest)
	if strings.Contains(normalizedQuery, normalizedName) || strings.Contains(normalizedName, normalizedQuery) {
		score = max(score, 0.8+0.2*float64(min(len(normalizedQuery), len(normalizedName)))/float64(longest))
	}
	queryTokens := nameTokens(query)
	score = max(score, tokenOverlap(queryTokens, nameTokens(definition.Name)))
	score = max(score, 0.7*tokenOverlap(queryTokens, nameTokens(definition.Description)))
	return score
}

// normalizeName lowercases value and strips separators so agentUpdatePlan and
// agent_update_plan compare as the same symbol.
func normalizeName(value string) string {
	normalized := make([]rune, 0, len(value))
	for _, current := range strings.ToLower(value) {
		if unicode.IsLetter(current) || unicode.IsDigit(current) {
			normalized = append(normalized, current)
		}
	}
	return string(normalized)
}

// nameTokens splits an identifier or description into lowercase words on
// snake_case, kebab-case, and camelCase boundaries.
func nameTokens(value string) []string {
	var tokens []string
	var current []rune
	runes := []rune(value)
	flush := func() {
		if len(current) >= 2 {
			tokens = append(tokens, strings.ToLower(string(current)))
		}
		current = current[:0]
	}
	for index, letter := range runes {
		if !unicode.IsLetter(letter) && !unicode.IsDigit(letter) {
			flush()
			continue
		}
		if unicode.IsUpper(letter) && index > 0 {
			previous := runes[index-1]
			if unicode.IsLower(previous) || unicode.IsDigit(previous) {
				flush()
			}
		}
		current = append(current, letter)
	}
	flush()
	return tokens
}

// tokenOverlap reports the fraction of query tokens present in candidate.
func tokenOverlap(queryTokens, candidate []string) float64 {
	if len(queryTokens) == 0 {
		return 0
	}
	known := make(map[string]struct{}, len(candidate))
	for _, token := range candidate {
		known[token] = struct{}{}
	}
	matched := 0
	for _, token := range queryTokens {
		if _, ok := known[token]; ok {
			matched++
		}
	}
	return float64(matched) / float64(len(queryTokens))
}

// levenshtein returns the edit distance between left and right in runes.
func levenshtein(left, right string) int {
	leftRunes, rightRunes := []rune(left), []rune(right)
	previous := make([]int, len(rightRunes)+1)
	current := make([]int, len(rightRunes)+1)
	for index := range previous {
		previous[index] = index
	}
	for i := 1; i <= len(leftRunes); i++ {
		current[0] = i
		for j := 1; j <= len(rightRunes); j++ {
			cost := 1
			if leftRunes[i-1] == rightRunes[j-1] {
				cost = 0
			}
			current[j] = min(min(current[j-1]+1, previous[j]+1), previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(rightRunes)]
}

func rejectedCall(call Call, err error) Result {
	result := Result{
		ToolCallID: call.ID, Name: call.Name,
		Content: fmt.Sprintf("%s rejected: %v", call.Name, err), IsError: true,
	}
	result.SyncLegacyContent()
	return result
}

func (b *Bus) ExecuteBatch(ctx context.Context, calls []Call, mode Mode, options ExecuteOptions) ([]Result, error) {
	if len(calls) > MaxBatchCalls {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyToolCalls, len(calls), MaxBatchCalls)
	}
	if mode == ModeParallel && !b.requiresSequential(calls) {
		options.Sink = synchronizeUpdateSink(options.Sink)
		return b.executeParallel(ctx, calls, options)
	}
	results := make([]Result, 0, len(calls))
	for index, call := range calls {
		result, err := b.Execute(ctx, call, options)
		if err != nil {
			failures := make([]CallExecutionError, 0, len(calls)-index)
			failures = append(failures, CallExecutionError{CallID: call.ID, Err: err})
			for _, skipped := range calls[index+1:] {
				failures = append(failures, CallExecutionError{
					CallID: skipped.ID,
					Err:    errors.Join(ErrNotExecuted, context.Canceled),
				})
			}
			return results, &BatchExecutionError{Failures: failures}
		}
		results = append(results, result)
	}
	return results, nil
}

func (b *Bus) requiresSequential(calls []Call) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, call := range calls {
		if b.definitions[call.Name].Concurrency == ConcurrencySequential {
			return true
		}
	}
	return false
}

func (b *Bus) executeParallel(ctx context.Context, calls []Call, options ExecuteOptions) ([]Result, error) {
	results := make([]Result, len(calls))
	errs := make([]error, len(calls))
	workerCount := min(DefaultBatchConcurrency, len(calls))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				current := calls[index]
				if err := ctx.Err(); err != nil {
					errs[index] = errors.Join(ErrNotExecuted, err)
					continue
				}
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							errs[index] = fmt.Errorf("%w: %s: %v", ErrToolPanic, current.Name, recovered)
						}
					}()
					results[index], errs[index] = b.Execute(ctx, current, options)
				}()
			}
		}()
	}
dispatch:
	for index := range calls {
		if ctx.Err() != nil {
			for skipped := index; skipped < len(calls); skipped++ {
				errs[skipped] = errors.Join(ErrNotExecuted, ctx.Err())
			}
			break
		}
		select {
		case jobs <- index:
		case <-ctx.Done():
			for skipped := index; skipped < len(calls); skipped++ {
				errs[skipped] = errors.Join(ErrNotExecuted, ctx.Err())
			}
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()
	failures := make([]CallExecutionError, 0)
	succeeded := make([]Result, 0, len(results))
	for index, result := range results {
		if errs[index] != nil {
			failures = append(failures, CallExecutionError{CallID: calls[index].ID, Err: errs[index]})
			continue
		}
		if result.ToolCallID == "" {
			result.ToolCallID = calls[index].ID
		}
		if result.Name == "" {
			result.Name = calls[index].Name
		}
		succeeded = append(succeeded, result)
	}
	if len(failures) > 0 {
		return succeeded, &BatchExecutionError{Failures: failures}
	}
	return results, nil
}

func cloneDefinition(definition Definition) Definition {
	definition.InputSchema = cloneSchema(definition.InputSchema)
	return definition
}

func cloneSchema(schema Schema) Schema {
	schema.Required = slices.Clone(schema.Required)
	schema.Enum = slices.Clone(schema.Enum)
	if schema.Properties != nil {
		schema.Properties = maps.Clone(schema.Properties)
		for name, child := range schema.Properties {
			schema.Properties[name] = cloneSchema(child)
		}
	}
	if schema.Items != nil {
		item := cloneSchema(*schema.Items)
		schema.Items = &item
	}
	if schema.AdditionalProperties != nil {
		additional := *schema.AdditionalProperties
		schema.AdditionalProperties = &additional
	}
	return schema
}
