# Kernel optimization with Ralph

Use reference measurements to guide each change, reopen architecture decisions when progress
stalls, and carry forward only accepted kernels and independently verified knowledge.

These instructions apply to the owning kernel task. Read its frozen contract and saved evidence
at every fresh Ralph activation. Execution authority and acceptance remain with that task's
existing infrastructure and verifier.

1. Keep the operator semantics, shapes, precision, reference, judge, measurement protocol, budget,
   and stopping rule fixed. Preserve the accepted candidate before changing code.
2. Inspect the reference's available timing and profiler evidence before choosing an optimization.
   Distinguish observations from architectural hypotheses. Request a focused probe when the
   evidence cannot distinguish explanations; do not invent a hardware fact.
3. State the next hypothesis, its triggering observation, the proposed change, and the measurement
   that would support or reject it. Change one decision at a time, except for a documented atomic
   group whose parts must work together. Establish the architecture before local parameter tuning.
4. Use the task's authorized execution path and canonical verifier. Check correctness before
   comparing performance, measure reference and candidate in the same job, and retain source
   identity and raw output. A self-reported score cannot accept a candidate or prove a mechanism.
5. When the frozen stagnation rule fires, or the available decisions cannot close a gate, revisit
   decomposition, dataflow, tile layout, resource use, and pipeline structure. Use a discriminating
   probe before more tuning. Reopen the earliest affected decision and revalidate its dependents.
6. Record each attempt's hypothesis, candidate, evidence, verdict, and next action in the task's
   existing record. Revert rejected changes to the accepted candidate. Infrastructure failures and
   unknown jobs are not optimization failures; reconcile the exact job before retrying.
7. Treat explanations as knowledge proposals until an independent probe verifies the mechanism
   on the named hardware. Reuse certified entries within their scope. Preserve measured negative
   and null results with their retry preconditions; do not turn missing evidence into a no-go.
8. Promote a candidate to the next reference only after the task's independent confirmation gate
   passes. Retain the original reporting baseline. If source isolation is part of the contract,
   require its enforcement across generations; a fresh Ralph session alone does not provide it.
9. Stop at the frozen budget or stopping condition, or report the missing capability or unresolved
   evidence. State the best accepted result and the stop reason. A plateau does not prove hardware
   optimality, and process exit does not prove device-job completion or resource release.
