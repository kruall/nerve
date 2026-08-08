# Placement forward tests

Test without an expected answer: a compiler wrapper, a Git capability, Nerve
session cancellation with persisted continuation/UI state, and a third-party
HTTP integration. The compiler, Git, and third-party integration default to
external isolated servers; session cancellation is embedded because Nerve owns
its lifecycle and persistence. "Nerve will consume it" fails the gate.
