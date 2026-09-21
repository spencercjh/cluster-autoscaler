# HAMi node-local feasibility (alpha)

This optional adapter sends the current snapshot's complete resident Pod set to
an extended HAMi Filter implementation. It is intended for a matched pair of CA
and HAMi fork builds; `hami.io/feasibility-v1alpha1` is not an upstream API.

Configure the normal scheduler extender, with `filterVerb` and explicit
`managedResources`, then select its exact `urlPrefix` with
`--hami-feasibility-extenders=https://hami-scheduler:443`.
The selected extender must not be ignorable. Its HTTP timeout and TLS client
configuration apply. TLS verification remains enabled unless explicitly disabled
in that configuration. The adapter always sends full Nodes, independently of
`nodeCacheCapable`, and does not change the production scheduler configuration.

A missing acknowledgement, unsupported resource declaration, malformed response,
transport failure, or incomplete snapshot rejects the simulation. Every response
must account for every candidate node. A request or response exceeding 1 MiB is
rejected, matching the HAMi Filter request limit; occupancy is never truncated.
Other extenders retain the standard Kubernetes wire contract.

Real bound Pods retain their recorded NVIDIA device allocation. Successful
simulated placements, nominated unbound Pods, and Pods copied into templates
request fresh allocation while keeping user device constraints. Provenance is
stored with PodInfo and follows the snapshot's normal copy, remove, fork, revert,
and commit operations. HAMi annotation interpretation stays in this adapter.

The matching HAMi implementation supports NVIDIA hami-core inventory only. Its
result describes node-local feasibility, excluding namespace quota, remote
reservation and device execution. It replays preserved allocations first and
other residents in stable Pod UID order, then evaluates the candidate. This
strategy can conservatively reject a placement that a different ordering could
fit. MIG and other device backends are unsupported. A zero-node template lacking
registered device inventory cannot establish feasibility. Template device IDs
are scoped to that candidate node, not evidence of a future machine's UUIDs.

Local tests cover the transport and snapshot lifecycle. The real two-repository
HTTP integration must also run against the matching HAMi checkout before using
remote images. No local unit test proves real GPU execution or cloud scaling.
