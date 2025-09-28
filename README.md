# dsim

Deterministic simluation testing framework for distributed systems (and also awesome for async games).

## MessageBus

Effectively a managed simlulator, runs a constant tick rate and handles real time message passing.

## Simulator

Unmanaged simulator, requires the user to step by a specific tick (`step_by()`), or to a time by a specific tick (`step_to()`).
