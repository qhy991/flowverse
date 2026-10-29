# ForgeKernel implementation notes

The personal `forge_kernel` flow is implemented and released as `0.1.0` in
[qhy991/forge-kernel-flow](https://github.com/qhy991/forge-kernel-flow/tree/v0.1.0).
The [manifest in this fork](../../flows/qhy991/forge_kernel/0.1.0/flow.yaml) pins its tested source
commit. The source repository owns executable behavior, schemas, worker instructions, tests,
and usage. This page records the design choice and the evidence boundary.

## Selected shape

One Ralph-style worker takes fresh architecture and implementation activations. A bounded
controller decides the next phase from stored evidence. It does not invoke an unbounded Ralph
loop inside another loop, and it does not require a second model.

The controller owns one protected, atomically written campaign record. Model replies propose
plans and implementations. A fixed deployment-owned executable supplies measurement receipts,
source snapshots, restoration, confirmation, and reference seals. Models cannot choose that
trusted executable or fill accepted timing fields.

The flow follows this sequence:

1. Bind the frozen task, actual worker/environment identities, permitted coding seed, and executor.
2. Obtain source-safe reference observations and propose a decision DAG with evidence and atomic groups.
3. Submit a complete candidate, freeze its source, and run independent correctness and paired measurement.
4. Retain a structural incumbent or measured improvement; restore rejected changes from a permitted seed
   or current-generation incumbent, never from the sealed reference source.
5. Reopen architecture after eligible measured stagnation. Record changed decisions and their dependent
   atomic groups, and require the next implementation to rebuild them.
6. Independently confirm the selected candidate in a new job before sealing it as the next reference.
7. Admit a mechanism to the private knowledge catalog only after an independent, source-safe probe.
   Later plans may cite only the pinned certified knowledge and current-generation admitted observations.

Gain means `reference_time / candidate_time - 1`. The frozen task supplies its noise rule and
promotion target. The original reporting anchor remains fixed across generations. Failed and
unresolved evidence stays recorded. A bounded plateau is not proof of hardware optimality.

## Runtime and recovery

The implementation pins public Humanize commit
[`58dd43a`](https://github.com/humanfia/humanize/tree/58dd43a419b9d1967c8028f3b999192b040b7b60).
Its async flow is executed through declared agents, environments, params, and `FlowContext`.
Package `agent.md` is explicitly included in every fresh worker prompt, together with frozen
custom task instructions. The separate [plain-Ralph template](../ralph-loop/agent.md) remains
available for users who want instructions without the executable controller.

The runtime's journal is a progress projection. The campaign record and immutable requests and
receipts own recovery. Before an evaluator effect, the controller commits its full request and
stable operation ID. Resume reconciles that exact effect before more work. A crash after receipt
publication cannot turn an unknown job into permission for a new submission.

Humanize resumes with a fresh per-run budget. The controller records actual runtime usage deltas
and requires trusted remaining-allowance evidence bound to durable history. It does not estimate
model prices. A model turn without a durable reply remains blocked rather than being replayed.
The source README documents this limitation and the absence of a manual audit-resolution command.

## Verification and remaining boundary

Release `0.1.0` passed 39 repository checks through the real pinned runtime, 43 independent
adversarial checks including the original knowledge-reuse failure, and 6 extra evidence-boundary
checks. The built wheel included worker instructions and completed a two-generation demonstration
from outside the source checkout.

The demonstration uses scripted model replies, CPU correctness checks, and synthetic timing data.
These establish software control and recovery behavior. They do not establish native source
isolation, paid-model behavior, GPU correctness, or a measured optimization benefit.

A real task must supply its authorized native executor and verifier. Different directories,
fresh sessions, or a permission declaration alone do not prove isolation. Each generation needs
new deployment admission. Missing capability or unresolved work remains an explicit blocked result.
See the [executor protocol](https://github.com/qhy991/forge-kernel-flow/blob/v0.1.0/examples/protocol.md)
for the actual request/receipt contract, rather than the earlier proposed interfaces.

## Placement and delivery

Executable source lives in the personal source repository. This fork holds its immutable release
manifest, installation entry, and these notes. [Install from this fork](../../README.md#personal-forgekernel).
No official-index registration or upstream PR is authorized by this delivery. Any future upstream
submission requires a new explicit user request.

## Method source

The user-supplied manuscript, *ForgeKernel: Self-Evolving Kernels Catching and Surpassing Experts
and Agents*, sections 3.1-3.5 and appendices B, C, and G, motivates reference analysis,
architecture reopening, measured gates, reference promotion, and certified knowledge reuse.
The personal single-worker controller, protected record, native-executor protocol, and explicit
uncertainty handling are engineering choices here. This is not a claim to reproduce the paper's
hardware results. The initial proposal remains available in this file's Git history.
