package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/skill"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

// ErrProviderResolverMissing is returned by Build when BuildDeps carries no
// provider.Resolver — the Engine cannot issue a model call without one.
var ErrProviderResolverMissing = errors.New("agent: build deps missing provider resolver")

// ErrSkillRegistryMissing is returned by Build when Spec.Skills names active
// skills but BuildDeps carries no skill registry to resolve them.
var ErrSkillRegistryMissing = errors.New("agent: build deps missing skill registry")

// Spec declares how to materialize one Agent Engine. It contains executable
// model, tool, skill, context, and loop defaults only; identity, routing, and
// per-call input/output contracts belong to the application.
type Spec struct {
	// Instructions is the agent's system prompt. When BuildDeps supplies no
	// ContextManager, Build wires a default one that seeds the loop with
	// Instructions as the system message and Request.Prompt/Content as the
	// user message.
	Instructions string

	// Skills names reusable instruction bundles resolved against BuildDeps.Skills
	// and injected into Engine.Run context.
	Skills []string

	// AvailableSkills names reusable instruction bundles disclosed to the model
	// as a compact catalog and activated on demand. Unlike Skills, their bodies
	// are not injected before activation.
	AvailableSkills []string

	// Provider optionally pins resolution to a driver Metadata().Name. Model is
	// the model name sent to that driver. FallbackModel is resolved separately
	// and tried only when the primary stream cannot be opened.
	Provider      string
	Model         string
	FallbackModel string

	// Temperature, TopP, and MaxTokens are forwarded to every provider request.
	// Zero leaves the selected provider's default in place.
	Temperature float64
	TopP        float64
	MaxTokens   int

	// Tools names the tools this agent may call. Build selects exactly this
	// subset from BuildDeps.Tools; an empty slice yields a tool-less agent.
	Tools []string

	// LoopPolicy bounds one Engine.Run (iterations, wall-clock, budget). A
	// per-request Budget still overrides it at run time.
	LoopPolicy LoopPolicy

	// ThinkingBudget caps provider reasoning tokens per turn; zero leaves the
	// provider default in place. StopSequences are forwarded to every turn.
	ThinkingBudget int
	StopSequences  []string

	// godoc-allow-any: provider-specific request extensions are intentionally open.
	ExtraBody map[string]any
}

// BuildDeps carries the live runtime dependencies a Spec cannot hold by value:
// the provider resolver, the master tool registry, the hook chain, and an
// optional ContextManager override. These are wired once per deployment and
// reused across every Build.
type BuildDeps struct {
	// Providers resolves Spec.Model to the driver that serves it. Required.
	// A single-provider deployment passes provider.Single(driver).
	Providers provider.Resolver

	// Tools is the master registry Build selects each Spec's named subset from.
	// May be nil only when every Spec being built declares no tools.
	Tools *tool.Bus

	// ContextSelection optionally supplies select_context when named in Spec.Tools.
	// Nil disables this capability. Credentials remain in live dependencies.
	ContextSelection *kit.ContextSelectionConfig

	// Skills is the registry Build uses to resolve Spec.Skills. It is required
	// only when a Spec declares one or more skill names.
	Skills *skill.Registry

	// Hooks is the hook chain installed on the materialized Engine. The zero
	// value is a valid empty chain.
	Hooks HookChain

	// ContextManager, when set, overrides the default instructions-based
	// context builder for every Engine built with these deps.
	ContextManager ContextManager
}

// Build materializes a Spec into an Engine using the supplied dependencies. It
// is the single construction entry point for the agent layer.
//
// Build resolves Spec.Model through deps.Providers to pick the driver, selects
// the named tool subset from deps.Tools (failing if a named tool is absent, so
// a misdeclared tool fails at construction rather than mid-run), and wires
// Spec.Instructions into a default ContextManager unless deps overrides it. A
// missing resolver or an unservable model fails here rather than at the first
// model call.
func Build(spec Spec, deps BuildDeps) (Engine, error) {
	if deps.Providers == nil {
		return Engine{}, ErrProviderResolverMissing
	}
	driver, err := provider.Resolve(deps.Providers, spec.Provider, spec.Model)
	if err != nil {
		return Engine{}, fmt.Errorf("agent: resolve provider %q for model %q: %w", spec.Provider, spec.Model, err)
	}
	if spec.FallbackModel != "" {
		fallback, fallbackErr := provider.Resolve(deps.Providers, "", spec.FallbackModel)
		if fallbackErr != nil {
			return Engine{}, fmt.Errorf("agent: resolve fallback model %q: %w", spec.FallbackModel, fallbackErr)
		}
		driver = provider.ModelFallback(driver, fallback, spec.FallbackModel)
	}

	registry := deps.Tools
	if deps.ContextSelection != nil && slices.Contains(spec.Tools, "select_context") {
		selection, err := kit.ContextSelectionTool("select_context", *deps.ContextSelection)
		if err != nil {
			return Engine{}, err
		}
		registry = registry.Clone()
		if err := registry.Register(selection); err != nil {
			return Engine{}, err
		}
	}
	bus, err := resolveBuildTools(spec.Tools, registry)
	if err != nil {
		return Engine{}, err
	}
	activeSkills, availableSkills, err := resolveBuildSkills(spec.Skills, spec.AvailableSkills, deps.Skills)
	if err != nil {
		return Engine{}, err
	}

	contextManager := deps.ContextManager
	if contextManager == nil {
		contextManager = instructionsContext{instructions: spec.Instructions}
	}
	loopPolicy := spec.LoopPolicy
	loopPolicy.Budget = cloneBudget(spec.LoopPolicy.Budget)
	loopPolicy.SessionBudget = cloneSessionBudget(spec.LoopPolicy.SessionBudget)
	loopPolicy.ModelTimeouts = cloneModelTimeouts(spec.LoopPolicy.ModelTimeouts)

	return Engine{
		Provider:        driver,
		Tools:           bus,
		Hooks:           deps.Hooks,
		Model:           spec.Model,
		Temperature:     spec.Temperature,
		TopP:            spec.TopP,
		ModelMaxTokens:  spec.MaxTokens,
		LoopPolicy:      loopPolicy,
		ContextBuilder:  contextManager,
		Skills:          activeSkills,
		AvailableSkills: availableSkills,
		ThinkingBudget:  spec.ThinkingBudget,
		StopSequences:   slices.Clone(spec.StopSequences),
		ExtraBody:       cloneAnyMap(spec.ExtraBody),
	}, nil
}

func resolveBuildTools(names []string, registry *tool.Bus) (*tool.Bus, error) {
	if len(names) == 0 {
		return nil, nil
	}
	if registry == nil {
		return nil, fmt.Errorf("agent: spec lists %d tool(s) but build deps carry no tool bus", len(names))
	}
	if err := registry.Validate(); err != nil {
		return nil, fmt.Errorf("agent: invalid tool registry: %w", err)
	}
	for _, name := range names {
		if _, ok := registry.Driver(name); !ok {
			return nil, fmt.Errorf("agent: tool %q named by spec is not registered in build deps", name)
		}
	}
	subset := registry.Subset(names)
	if err := subset.Validate(); err != nil {
		return nil, fmt.Errorf("agent: invalid tool subset: %w", err)
	}
	return subset, nil
}

func resolveBuildSkills(activeNames, availableNames []string, registry *skill.Registry) ([]skill.Skill, []skill.Skill, error) {
	if len(activeNames) == 0 && len(availableNames) == 0 {
		return nil, nil, nil
	}
	if registry == nil {
		return nil, nil, fmt.Errorf(
			"%w: spec lists %d skill(s)",
			ErrSkillRegistryMissing,
			len(activeNames)+len(availableNames),
		)
	}
	active, err := registry.Resolve(activeNames...)
	if err != nil {
		return nil, nil, wrapSkillResolveError(err)
	}
	available, err := registry.Resolve(availableNames...)
	if err != nil {
		return nil, nil, wrapSkillResolveError(err)
	}
	activeSet := make(map[string]struct{}, len(active))
	for _, current := range active {
		activeSet[current.Name] = struct{}{}
	}
	filtered := available[:0]
	for _, current := range available {
		if _, alreadyActive := activeSet[current.Name]; !alreadyActive {
			filtered = append(filtered, current)
		}
	}
	return active, filtered, nil
}

func wrapSkillResolveError(err error) error {
	var missing *skill.NotFoundError
	if errors.As(err, &missing) {
		return fmt.Errorf("agent: resolve skill %q: %w", missing.Name, err)
	}
	return fmt.Errorf("agent: resolve skills: %w", err)
}

// instructionsContext is the default ContextManager installed by Build.
type instructionsContext struct {
	instructions string
}

func (c instructionsContext) Build(_ context.Context, request Request) ([]message.Message, error) {
	system := strings.TrimSpace(c.instructions)
	if system == "" {
		system = "You are a Venat agent."
	}
	input, err := requestMessage(request)
	if err != nil {
		return nil, err
	}
	return []message.Message{
		message.NewText(message.RoleSystem, system),
		input,
	}, nil
}

func (instructionsContext) Compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	return history, nil
}
