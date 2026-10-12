# Harness/API additions requested by the CON and NET suites

Branch `tests/con-net`. All additions are **stubs** (`Stub.NotYet`) in new partial-class files; no existing harness or SDK
file or signature was changed. Each entry lists the exact signature, the required behaviour, and the tests that rely on it.

## `TestCluster` — `tests/Tedd.Tdfs.Testing/Additions/con-net/TestCluster.con-net.cs`

### `Task TransferLeadershipAsync(string group, string targetNode, CancellationToken ct = default)`

Transfers leadership of a Raft group (`"control"` or a volume name) to a chosen running voter through the production
path (`RaftNode.TransferLeadership` → `TimeoutNow` once the target is caught up); never edits persisted state.
Completes as soon as the target reports `RaftRole.Leader` for the group in a term higher than the one observed before the
call (it does **not** wait for the new leader to apply earlier committed entries). Returns immediately when the target
already leads. `ArgumentException` for an unknown group or a target that is not a voter of it,
`InvalidOperationException` when the target is not running, `TimeoutException` (naming group, target, observed
leader/term) when not achieved within 10 election timeouts of the cluster's `NodeTimings`. Virtual time in deterministic
mode; recorded in `Trace`.

Used by: CON-009 (`apply-delayed-new-leader-by-transfer`), CON-012, CON-013, CON-015, NET-014 (`node-moves-to-new-endpoint`).

### `Task<NodeHost> StartForeignClusterNodeAsync(string listenEndpoint, CancellationToken ct = default)`

Starts a single-node cluster with a **different** `ClusterId` and cluster secret on an endpoint of this cluster's
network (typically the `Endpoint` of a crashed node), so that a cached address leads to a foreign cluster. The foreign
node has a volume named like this cluster's first volume and bootstraps the same principal names/secrets (including
`admin`), so only the cluster-identity check can tell the clusters apart. It attaches to this cluster's transport
(in-memory/simulated or QUIC), uses the same `NodeTimings`, clock mode and fault/network controls, and is disposed with
the cluster. While it runs, a `TestClientOptions.SeedNodes` entry naming the node whose endpoint it took resolves to the
foreign node's endpoint. `InvalidOperationException` when a running node of this cluster still listens on the endpoint.

Used by: NET-014 (`endpoint-taken-by-foreign-cluster`).

## `TestNetwork` — `tests/Tedd.Tdfs.Testing/Additions/con-net/TestNetwork.con-net.cs`

### `MessageRule TruncateNext(MessageFilter filter, int keepBytes, int count = 1)`

For the next `count` matching frames, only the first `keepBytes` bytes reach the receiver, then the carrying connection
is closed (end-of-stream inside a frame). In-memory/simulated transports hand the receiver's transport layer the partial
frame followed by a close; QUIC writes the partial bytes and closes from the sending side. `keepBytes` ≥ frame length
delivers the frame and then closes. `MessageRule.Matched` counts truncated frames; recorded in `Trace`.
`ArgumentOutOfRangeException` for negative `keepBytes` or nonpositive `count`.

Used by: NET-007 (reply-truncation variants).

### `void KillConnectionsSilently(string name)`

Silently kills every connection currently established by or to `name` (`"n0"`, `"b0"`, `"client:c0"`): no close/reset is
delivered to either endpoint, every frame later written on those connections vanishes in both directions, and the
connection objects keep reporting themselves open until the owner's own deadline/keep-alive logic detects the failure.
New connections over the same links work normally. Recorded in `Trace`; unknown names throw `ArgumentException`.

Used by: NET-016 (`idle-connection-silently-dead`, `connection-dies-with-reply-in-flight`).

## `RawProtocolClient` — `tests/Tedd.Tdfs.Testing/Additions/con-net/RawProtocolClient.con-net.cs`

### `Task<DfsErrorCode> SendFrameInPiecesAsync(ReadOnlyMemory<byte> frame, IReadOnlyList<int> splitOffsets)`

Sends one complete application frame split into several transport writes at the given offsets (strictly increasing, in
`(0, frame.Length)`), letting the receiver process each piece before the next one, as **one** logical request (one
correlation id) through the real envelope/fragmentation path (in-memory/simulated: one fragment per piece with a
scheduler turn in between; QUIC: each piece written and flushed separately). Returns the error code of the server's
reply (`None` for success, `ProtocolError` when the server closes). Invalid offsets → `ArgumentException`; an empty list
behaves like `SendFrameAsync`.

Used by: NET-006.

### `Task<DfsErrorCode> SendPartialFrameAndCloseAsync(ReadOnlyMemory<byte> frame, int deliveredBytes)`

Writes only the first `deliveredBytes` (in `[0, frame.Length)`) of a frame as a transport message that announces the full
length, then closes the connection. Completes only after the server observed the close; returns the error code the
server reported before closing, or `ProtocolError` when it only closed. Afterwards `IsConnected` is false.
Out-of-range `deliveredBytes` → `ArgumentOutOfRangeException`.

Used by: NET-007 (`request-truncated-then-eof`).

## Folder-name deviation (Windows reserved name)

API.md §G asks for `<Suite>/<Suite><Level>Tests.cs`, i.e. a `Con/` folder for the CON suite. `CON` is a reserved device
name on Windows: Git for Windows cannot index files below a `Con/` directory (`open(".../Con/...")` fails), so the CON
files live in `Consensus/` instead (`tests/Tedd.Tdfs.Tests.Models/Consensus/ConModelsTests.cs`,
`tests/Tedd.Tdfs.Tests.Simulation/Consensus/ConSimulationTests.cs`). Namespaces (`Tedd.Tdfs.Tests.<Level>.Con`), class names
(`ConModelsTests`, `ConSimulationTests`), file names and method names follow the convention unchanged.

## Conventions the tests rely on (no new members)

These are documented here because the CON/NET tests depend on them; they follow from API.md/ARCHITECTURE.md but are
spelled out so the implementation and the harness agree:

* `FaultContext.Operation` on request-path sites (F00–F15) is the protocol operation kind of the message being handled
  (`MessageTypes.OperationOf`), e.g. `PutObject` at `object.*` sites, `WriteAll` at `raft.*` sites for a WriteAll entry.
  NET-020's matrix matches request-path sites on the operation and background sites (F16–F21) on the site alone.
* `FaultAction.Delay(TimeSpan.Zero)` is used as a pure observer arm (count hits, `AssertReached`) without changing behaviour.
* `TestNetwork.DropReplies(filter, …)`: the filter selects the **request** (`From` = client, `Type` = request type); the
  request is delivered and executed and its reply is lost.
* `MessageLog` counts delivered messages per filter; `MessageHold.Captured` keeps frames for `ReplayAsync`.
* `TestCluster.Network.Partition(...)` groups may list client network names (`client:<name>`); a client is only able to
  reach nodes in its own group.
