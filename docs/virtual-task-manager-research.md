# Virtual task manager: research proposal

## Status

This document records the initial proposal and research questions. It is not an
implementation design or a commitment to extend the current Nerve task model.

## Desired outcome

Nerve should be able to help a person turn intentions into verifiable outcomes,
decompose them into manageable work, represent uncertainty, and track how the
forecast changes over time. The system should preserve manual control over
priorities and execution order.

The target is not merely a smarter task list. It is a model of commitments with
explicit ownership, scope, dependencies, uncertainty, history, and current
forecast.

## First boundary to research: whose task is it?

Nerve currently has one persistent task collection backed by Markdown files and
a SQLite search index. A task has a status, deadline, source, tags, and free-form
content, but no explicit owner, workspace, hierarchy, dependency graph, estimate,
or planning baseline.

Before extending this model, the research must distinguish at least:

- a user commitment: something the human intends to accomplish;
- an assistant commitment: autonomous or delegated work Nerve owes the user;
- an operational action: internal work needed to run a session, workflow, cron
  job, or execution;
- a shared project item: work whose state matters to both the user and the
  assistant.

These categories may share storage primitives, but must not be silently mixed in
inboxes, progress reports, timelines, reminders, or capacity calculations.

Candidate approaches to compare:

1. Separate domain models and stores.
2. One work-item model with explicit owner, actor, workspace, visibility, and
   purpose.
3. A user-facing project model that references existing Nerve tasks used for
   assistant execution.

The choice should be based on concrete end-to-end scenarios rather than schema
convenience. A single universal work-item model is only acceptable if it keeps
the user-facing and operational views unambiguous.

## Conceptual work hierarchy

The initial hierarchy is:

```text
Area
└── Goal
    └── Project
        ├── Outcome or milestone
        │   └── Work package
        │       └── Next action
        └── Outcome or milestone
```

SMART is primarily a contract for goals, project outcomes, and milestones. It
should not be mechanically applied to every next action.

A SMART contract should capture:

- the expected outcome;
- the initial and target states;
- a metric or binary completion condition;
- a deadline or time window;
- relevance to a larger goal;
- constraints, scope, and explicit non-goals;
- acceptable evidence of completion.

Research work needs a variant: a question, a time budget, and the decision or
artifact expected at the end.

## Decomposition and relationships

Decomposition continues until a work package is verifiable, assignable, small
enough to estimate, and does not hide a major unresolved decision.

Relationships are conceptually a continuation of the work-item model, not a
separate user-facing feature. They remain a distinct technical subdomain because
dependency graphs have their own invariants, queries, and failure modes.

Candidate relation types:

- parent / child;
- blocks / blocked by;
- requires;
- related;
- alternative.

The model must detect cycles in blocking relations and explain why an item is or
is not currently actionable.

## PERT: confirmed skill and MCP split

PERT must be implemented as both a skill and an MCP capability.

The skill should:

- conduct the estimation conversation;
- ask about optimistic, most likely, and pessimistic scenarios;
- expose assumptions and sources of uncertainty;
- avoid presenting invented numbers as user estimates;
- interpret the result and identify estimates that need decomposition or risk
  reduction.

The MCP should:

- persist O/M/P values, units, assumptions, author, and timestamp;
- calculate expected duration and variance deterministically;
- preserve revisions rather than overwrite history;
- record remaining estimates and actual duration;
- provide historical data for later calibration.

For one estimate:

```text
expected = (optimistic + 4 * most_likely + pessimistic) / 6
sigma = (pessimistic - optimistic) / 6
```

PERT estimates work; they do not by themselves determine calendar dates.

## Capacity: required separate research

A forecast requires some representation of available capacity, but the correct
model is not yet known. The research should examine:

- working hours and calendar exceptions;
- allocation across projects and life areas;
- existing commitments;
- context switching and limits on parallel work;
- external waiting time versus active effort;
- whether capacity belongs to a person, agent, shared resource, or project;
- how much detail can be maintained without turning planning into bookkeeping.

The first version may need a deliberately coarse capacity model rather than a
precise calendar.

## Scheduling: manual authority, computed consequences

Automatic selection of execution order is not a current goal. The user should
retain authority over priority, sequencing, and trade-offs.

The useful deterministic capability may instead be a forecast or feasibility
engine that:

- accepts a manually chosen order and explicit dependencies;
- calculates likely dates and uncertainty ranges;
- detects impossible deadline or capacity constraints;
- shows the consequences of moving, pausing, or resizing work;
- explains why a forecast changed;
- optionally identifies a critical dependency chain without automatically
  scheduling the user's work.

The research should decide whether this is a constrained planning engine, a
forecast engine, or simply a family of calculations. Naming it a scheduler too
early risks implying authority the system should not have.

## Baseline, actual, and forecast

Replanning requires three distinct views:

- baseline: the explicitly accepted plan at a point in time;
- actual: recorded starts, completions, blocks, scope changes, and elapsed work;
- forecast: the current projection based on remaining work and current
  constraints.

Material changes should be recorded as events. Reforecasting must not erase the
baseline or the reasons for deviation.

Open questions include baseline versioning, what requires explicit acceptance,
and whether small changes can be grouped into review checkpoints.

## Progress and timeline view: purpose before schema

The proposed project or timeline snapshot is not yet sufficiently defined. It
should not become a generic dashboard of percentages.

Research should begin with decisions the view must support:

- What should I work on now?
- Which commitment is at risk, and why?
- What changed since the last review?
- Which blocker or decision has the largest downstream impact?
- What must be deferred if a new commitment is accepted?
- How confident are we in a milestone date?

Possible inputs include completed work packages, achieved outcomes, remaining
estimates, time spent blocked, baseline deviation, and forecast movement. Manual
"percent complete" should not be treated as authoritative without a defined
meaning.

## Candidate capabilities

The initial proposal separates methodology from deterministic state changes and
calculations.

### Skills

- `task-management`: orchestration and review cadence;
- `smart-goal-design`: outcome contracts;
- `work-breakdown`: decomposition and dependency discovery;
- `pert-estimation`: estimation interview and interpretation;
- `capacity-review`: eliciting and maintaining usable capacity assumptions;
- `forecast-review`: interpreting consequences without taking sequencing
  authority from the user;
- `progress-review`: daily and weekly review;
- `portfolio-prioritization`: later cross-project trade-offs;
- `estimation-retrospective`: calibration against actual outcomes.

### Deterministic tool domains

- work-item storage and ownership;
- relation graph and validation;
- PERT estimate storage, calculation, and history;
- capacity and calendar assumptions;
- baseline and event history;
- forecast and feasibility calculations;
- project and timeline projections for UI and review skills.

Exact MCP operations should be designed only after the domain boundaries and
user/assistant ownership model are decided.

## Research deliverable

The research should produce:

1. An explicit ownership and isolation model for user, assistant, operational,
   and shared work.
2. End-to-end scenarios covering capture, decomposition, estimation, manual
   sequencing, execution, blocking, and replanning.
3. A domain model with lifecycle and invariants.
4. A responsibility map across skills, MCP tools, Nerve core, background jobs,
   and UI.
5. A decision on the minimum useful capacity and forecast models.
6. A definition of the timeline view in terms of user decisions it supports.
7. A narrow first version and a set of subsequent implementation tasks.

## Current decisions and open questions

Confirmed directions:

- PERT is a skill plus MCP capability.
- Capacity is necessary and needs focused research.
- Manual control owns priority and execution order.
- Baseline history is necessary for replanning.
- Relations continue the work-item model conceptually but may be implemented as
  a separate graph component.

Open questions:

- Should user and assistant tasks use separate stores, one typed model, or
  references between two models?
- Which concepts belong in the current task system, and which require a new
  project-planning domain?
- What is the smallest capacity model that improves forecasts without excessive
  maintenance?
- How much forecast computation is useful when sequencing remains manual?
- What decisions and actions should the timeline snapshot expose?
