# Tedd.TDFS — Wire Protocol Reference

Status: binding wire format of protocol version 1. Behaviour is specified in [ARCHITECTURE.md](ARCHITECTURE.md),
requirements in the [FSD](spec/distributed-filesystem-fsd.md), signatures in [API.md](API.md). This document defines
every frame that crosses a connection: its encoding, direction, fields, semantics, errors and idempotency. The
message records live in `src/Tedd.Tdfs.Core/Protocol/Messages` (namespace `Tedd.Tdfs.Protocol.Messages`); the field
tables in §9–§10 are generated from those records (wire order and wire types are taken from each record's decoder).

---

## 1. Layering and framing

* The transport (`IRpcTransport`, ARCHITECTURE §10) carries opaque payloads with its own envelope (request,
  response, one-way, fragments, correlation id). Fragmentation, reassembly, correlation and timeouts belong to the
  transport. **This document defines the payloads**: one application frame per transport message.
* **Frame** = `[u16 MessageType, little-endian][body]`. The body is the message record's fields in declaration order,
  followed by the message's **extension block** (§2.5). Decoding is strict: unknown type, malformed body or trailing
  bytes are protocol errors (§11).
* Requests have even type values (from 10); the success reply of request `T` is `T + 1`
  (`MessageTypes.ReplyOf`). Any request may instead be answered by `ErrorReply` (type 1). `RaftBatch` is normally
  sent one-way (no reply); every other request expects exactly one reply.
* `MessageTypes.OperationOf(type)` maps every type to a `DfsOperationKind` (used by fault injection and message
  filters).
  The node-internal restore steps `RestoreVolume`/`RestoreInstall` map to `Admin` (their committed entries and object
  writes are tagged `RestoreSnapshot`), so `RestoreSnapshot` identifies the client's request at the RPC fault sites.
* A reply never changes meaning with the connection it arrives on: correlation is the transport's job and a late reply
  to an abandoned request is discarded by the transport (NET-009).

## 2. Encoding

### 2.1 Primitive types

| Wire type | Encoding |
|---|---|
| `u8` | One byte. |
| `bool` | One byte, `0` or `1`; any other value is malformed. |
| `u16`/`u32`/`u64` (fixed) | Little-endian (used by the frame header, `list<u32>` elements and MAC inputs). |
| `varuint` | Unsigned LEB128, 1–10 bytes, **minimal** encoding (a trailing zero group is malformed), no 64-bit overflow. |
| `varint` | Signed 64-bit, zigzag (`(v << 1) ^ (v >> 63)`) then `varuint`. |
| `varint32` | `varint` whose value must fit a signed 32-bit integer. |
| `varuint16` | `varuint` whose value must be ≤ 65535. |
| `guid` | 16 bytes in the .NET little-endian GUID layout (`Data1` u32 LE, `Data2` u16 LE, `Data3` u16 LE, `Data4` 8 bytes). |
| `u128` | 16 bytes little-endian. |
| `bytes` | `varuint` length + bytes. Length ≤ `WireLimits.MaxBytesLength` (default 64 MiB) and ≤ remaining input. |
| `bytes[32]` / `bytes[0\|32]` | A `bytes` field whose length must be exactly 32 (or 0 or 32). |
| `string` | `varuint` byte length + **strict UTF-8** (invalid sequences rejected by decoders; unpaired surrogates rejected by encoders). Length ≤ `WireLimits.MaxStringBytes` (default 64 KiB). Strings are never normalized by the codec: paths are re-validated and NFC-normalized by the server (PATH-012). |
| `timestamp` | `varint` of UTC ticks (100 ns units since 0001-01-01), within the `DateTime` range. The UTC offset is not transmitted; decoders produce offset zero. |
| `duration` | `varint` ticks (may be negative; receivers validate). |
| `NodeId`, `VolumeId`, `SnapshotId` | `guid`. |
| `ObjectId` | `u128`. |
| `InodeId`, `HandleId`, `StagingId` | `varuint` (0 = none). |
| `OperationId` | `varuint` session, `varuint` sequence (both 0 = empty). |
| `TransactionId` | The `OperationId` of the transaction's begin. |
| `CommitToken` | `guid` cluster, `guid` volume, `varuint` epoch, `varuint` sequence (all zero = `CommitToken.None`). |

### 2.2 Composite types

| Wire type | Encoding |
|---|---|
| `T?` (optional) | `bool` presence flag, then `T` when present. |
| `list<T>` | `varuint` count, then the elements. Count ≤ `WireLimits.MaxCollectionCount` (default 2²⁰) **and** count × (minimum element size) ≤ remaining bytes, checked before allocation. Encoders write `null` lists as empty. |
| `set<string>` | A `list<string>` in strictly ascending ordinal order (`string.CompareOrdinal`, UTF-16 code units); unsorted or duplicate entries are malformed. Encoders sort. |
| `map<string, V>` | `varuint` count, then `(string key, V value)` pairs with keys strictly ascending in ordinal order. Encoders sort; decoders reject unsorted/duplicate keys and return ordinal sorted dictionaries (deterministic iteration on replicas). |
| record `R` | `R`'s fields in order, then `R`'s own extension block. |
| `enum E` | `varint` of the underlying value, range-checked against the underlying type; must be a **defined** member (decoders reject, encoders refuse to send). |
| `enum E (raw)` | As `enum E` but any representable value is accepted, so the receiver can answer a semantic error (for example `InvalidArgument` for an unknown `FileShare` bit) instead of a protocol error. Raw enums: `FileMode`, `FileAccess`, `FileShare`, `FileOptions`, `DfsPermissions`, `DfsErrorCode`, `DfsOperationKind`, `DfsVoterPlacement`, `WireEntryKinds`, `AdminOp`. |

### 2.3 Core value types

| Wire type | Fields in order |
|---|---|
| `Labels` (`DfsTopologyLabels`) | `string` region, zone, rack, host, device. |
| `StoragePolicy` (`DfsStoragePolicy`) | `varint32` TargetCopies, TargetDistinctZones, DurableCopiesBeforeAck, DurableZonesBeforeAck. Not validated by the codec (receivers answer `InvalidPolicy`). |
| `MetadataPolicy` (`DfsMetadataPolicy`) | `varint32` Voters, `enum DfsVoterPlacement (raw)` Placement, `list<string>?` ExplicitVoterNodes. |
| `VolumeLimits` (`DfsVolumeLimits`) | `varint` MaxFileLength, `varint` MaxPublishBytes, `duration` MaxTransactionDuration, `varint32` MaxTransactionTouchedInodes, `varint32` MaxTransactionDescriptorBytes, `varint` MaxTransactionStagedBytes, `varint32` MaxPinsPerSession, `varint32` MaxHandlesPerSession. |
| `ServerCapabilities` (`DfsServerCapabilities`) | `varint32` ProtocolVersion, `set<string>` Features, `varint` MaxFileLength, `varint32` MaxPathBytes, `varint32` MaxComponentBytes, `varint` MaxPublishBytes, `varint32` InlineThreshold, `varint32` ChunkSize, `varint32` MaxObjectSize, `VolumeLimits` TransactionLimits, `duration` LeaseDuration. |
| `Retention` (`DfsRetention`) | `timestamp?` ExpiresUtc, `bool` Hold. |
| `BackupCheckpoint` (`DfsBackupCheckpoint`) | `string` Target, `SnapshotId`, `string` SnapshotName, `CommitToken` CoveredCommit, `timestamp?` RetentionExpiryUtc, `bool` Held, `bool` IsExactSnapshot. |
| `Credentials` (`DfsCredentials`) | `string` Principal, `string` Secret. Only inside TLS-protected administrative requests; never logged (record `ToString` overrides redact secrets, SEC-016). |
| `Identity` (`DfsIdentity`, Addendum A) | `string` text form (`user:x`, `group:x`, `Everyone`, `AuthenticatedUsers`, `Administrators`, `CreatorOwner`); text that does not parse is malformed. |
| `AccessRule` (`DfsAccessRule`) | `Identity`, `varuint` rights (raw `DfsFileSystemRights` bits; receivers validate), `enum DfsAccessControlType (raw)`, `enum DfsInheritanceFlags (raw)`, `enum DfsPropagationFlags (raw)`, `bool` IsInherited, `string?` InheritedFrom. |
| `SecurityDescriptor` (`DfsSecurityDescriptor`) | `Identity` owner, `bool` AreAccessRulesProtected, `list<AccessRule>` rules. Not validated by the codec (receivers answer `InvalidArgument`). |

### 2.4 Canonical form

Every logical value has exactly one encoding: minimal varints, sorted sets and maps, strict booleans, exact-size
fixed fields. Consequently `encode(decode(frame)) == frame` for every frame a decoder accepts (including frames with
preserved extensions). This makes payload hashing (§5.2), MAC inputs and manifest identities stable (FMT-012).

### 2.5 Extension blocks and evolution

Every message body and every nested record ends with an extension block:

```
varuint count
count × ( varuint32 fieldId, u8 flags, bytes value )      flags: bit 0 = Required, bits 1-7 must be 0
```

* Field ids are unique within one block (duplicates are malformed). Version 1 defined **no** extension fields; fields added since are listed with their message (`RestoreSnapshot` field 1 `StoragePool`).
* An **unknown optional** extension is preserved on the decoded record (`Extensions`) and re-emitted when the record is
  re-encoded (for example when a node forwards a request), and is otherwise ignored (FMT-003).
* An **unknown required** extension makes the whole message fail with `Unsupported` (FMT-002): a receiver never
  silently ignores a field essential to correctness.
* Fields are added to a message that already shipped only as extensions (optional when old receivers may ignore them,
  required otherwise); a sender emits a required extension only when the corresponding feature is active
  cluster-wide (§12). Removing or reordering fields, or changing a type value, is a breaking change requiring a new
  protocol version.

### 2.6 Limits and hostile input (SEC-014/015)

Decoders are bounded by `WireLimits` (strings 64 KiB, byte fields 64 MiB, collections 2²⁰ elements by default) and by
the remaining input: a count that cannot possibly fit is rejected before any allocation, every element consumes at
least one byte, and no record is recursive, so a frame of *n* bytes allocates O(*n*). All malformed input raises
`DfsProtocolException` (`ProtocolError`, or `Unsupported` for a required extension); decoders never return partially
decoded messages and never throw other exception types.

## 3. Connection handshake and authentication

```
peer                                         server (live node or backup node)
 │ Hello {role, identity, protocol range, features, nonce}  ─▶
 │                                      ◀─ HelloChallenge {cluster, version, server identity, nonce, salt, iterations}
 │ HelloProof {proof over transcript}                          ─▶
 │                                      ◀─ HelloResult {accepted, principal, capabilities, server MAC}
 │ ... authenticated requests ...
```

* `Hello`/`HelloChallenge` and `HelloProof`/`HelloResult` are request/reply pairs on the new connection. Before a
  successful `HelloResult` the server accepts only these two requests; anything else is answered with
  `ErrorReply(AuthenticationFailed)` and the connection is closed. Anonymous access is impossible (SEC-002).
* **Cluster identity**: when `ExpectedClusterId` is set and differs from the server's cluster, the server answers
  `ErrorReply(ClusterMismatch)`. The peer also checks `HelloChallenge.ClusterId` itself and aborts with
  `DfsClusterIdentityException` (it must never send a proof to the wrong cluster). A backup node reports the source
  cluster it serves.
* **Protocol version**: the server selects the highest version in `[max(min_peer, min_server), min(max_peer,
  max_server)]`; an empty range is answered with `ErrorReply(Unsupported)`. `HelloChallenge.Features` lists the
  features active cluster-wide that the peer also supports; only those may be used on the connection.
* **Transcript**: `T = SHA-256("tdfs/hello/v1\0" ‖ u32le(len H) ‖ H ‖ u32le(len C) ‖ C)` where `H` and `C` are the
  complete `Hello` and `HelloChallenge` frames (type header included) exactly as transmitted. `T` binds both nonces,
  both identities, the protocol range and the features, so a proof cannot be replayed on another connection or
  reflected (`HandshakeAuth.ComputeTranscript`).
* **Principals** (role `Client`; also exporters authenticating to a backup node with export credentials) use the
  SCRAM-SHA-256 key schedule (RFC 5802/7677):
  `SaltedSecret = PBKDF2-HMAC-SHA256(UTF-8(secret), salt, iterations, 32)`,
  `ClientKey = HMAC(SaltedSecret, "Client Key")`, `StoredKey = SHA-256(ClientKey)`,
  `ServerKey = HMAC(SaltedSecret, "Server Key")`.
  The server stores only `(salt, iterations, StoredKey, ServerKey)` (`PrincipalVerifier`).
  * `HelloProof.Proof = ClientKey XOR HMAC(StoredKey, T)`; the server accepts iff
    `SHA-256(Proof XOR HMAC(StoredKey, T)) == StoredKey` (constant time).
  * `HelloResult.ServerMac = HMAC(ServerKey, T)`; the client verifies it **before sending any other request**
    (mutual authentication) and otherwise closes the connection with `DfsAuthenticationException`.
  * For an unknown principal the server returns the fake salt `HMAC(K, "tdfs/fake-salt/v1\0" ‖ UTF-8(principal))[0..16)`
    (K: a server-held key derived from the cluster or backup secret) with the default iteration count, and the proof then fails exactly like a wrong secret
    (`AuthenticationFailed`): principal existence is not disclosed. Clients refuse iteration counts outside
    `1..10 000 000`.
* **Nodes** (role `Node`, cluster secret): `NodeAuthKey = HKDF-SHA256(clusterSecret, info "tdfs/v1/node-auth")`
  (`KeyDerivation`); `Proof = HMAC(NodeAuthKey, "tdfs/node-proof/v1\0" ‖ T)`,
  `ServerMac = HMAC(NodeAuthKey, "tdfs/node-server/v1\0" ‖ T)` (distinct labels, so neither can be reflected). The
  `Hello` must carry the node's `NodeId`, name, incarnation and `BootId` (a random value chosen at every process
  start). Wrong secrets are rejected with `AuthenticationFailed` and removed node ids (CON-016) with `AccessDenied`. A
  node id that is not (yet) a member but proves the cluster secret is accepted only for the seed-join flow
  (`GetClusterMap`, `GetVolumeRoute`, `JoinCluster`); every other request on such a connection is answered
  `AccessDenied`. An incarnation older than the registered one, or equal to it with a different boot id (a second
  process started from a cloned disk), is rejected with `StaleIncarnation` (RSY-021). A newer, not yet registered
  incarnation is accepted, **including its Raft traffic**: committing the registration needs a control quorum, and
  after a full-cluster restart every voter is in that state. Its receipts are not trusted, and the node issues none,
  until `RegisterIncarnation` committed the (incarnation, boot id) pair (RCV-02).
* **Lagging views**: membership is judged against the receiver's local applied control state, which may lag (a new
  learner's view is empty until the log reaches it). Raft traffic (`RaftBatch`, `RaftSnapshotChunk`) is therefore also
  accepted from node ids that proved the cluster secret but are unknown to the receiver's view; removed and stale node
  identities are always rejected, and the Raft core itself ignores non-leader traffic from non-members. A node that knows
  no endpoint for a peer (a learner with an empty view) answers over the authenticated connection the peer opened.
* **Result**: on success the connection identity (`RpcPeerIdentity`: principal or node id/name/incarnation, role) is
  fixed and every later request is authorized against it, on every request (SEC-003). On rejection
  `HelloResult.Accepted = false` carries the error code and the server closes the connection.
* Implementations: `HandshakeAuth` (Core, internal, visible to Client and Server).

## 4. Routing, request header and authority

| Messages | Served by | Peer roles allowed |
|---|---|---|
| `Hello*`, `GetClusterMap`, `GetVolumeRoute` (without `RequireLeader`) | any live node; backup nodes serve their own map | all |
| `JoinCluster`, `RegisterIncarnation` | control leader | Node |
| `AdminRequest` | control leader (cluster/node/principal/feature ops) or volume leader (volume-scoped ops), §8.8 | Client (ClusterAdmin / VolumeAdmin) |
| volume requests (those with a `RequestHeader`) | volume leader; backup nodes for read-only snapshot traffic and their own sessions/pins | Client |
| `PutObject`, `GetObject`, `LocateObject` | any storage node (live node), backup node for its own objects | Client, Node |
| `ReplicateObject`, `DeleteObjects`, `CopyObjects` | storage node | Node (volume leader / restore coordinator) |
| `RestoreVolume` | control leader | Node (restore coordinator) |
| `BackupTargetUpdate` | control leader | Node (volume leader serving `BackupRegister`) |
| `RestoreInstall` | leader of the restoring volume | Node (restore coordinator) |
| `InventoryReport` | volume leader | Node |
| `RaftBatch`, `RaftSnapshotChunk` | group members | Node |
| `BackupBegin` … `BackupPublish`, `BackupRetention`, `BackupExportRestore` | backup node | Client authenticated as the export / retention / reader principal |

* A node that is not the leader of the addressed group answers `ErrorReply(NotLeader)` with `LeaderHint` (endpoint)
  and `Retryable = true`; the client refreshes its route and resends **the same frame** (same operation id). A
  minority leader cannot answer: it fails with `QuorumUnavailable`/`Timeout` rather than serving stale data
  (AT-06, CON-008).
* Messages reserved to node peers are rejected from clients with `AccessDenied`. Backup nodes reject every mutation
  with `ReadOnly` (BAK-015) and every lease/lock request (`OpenHandle`) with `ReadOnly` (BAK-002).

### 4.1 `RequestHeader`

Every volume-scoped request (39 types, all implementing `IVolumeRequest`) starts with:

| Field | Wire type | Semantics |
|---|---|---|
| `Volume` | `string` | Volume name. Unknown → `VolumeNotFound`. |
| `ExpectedVolumeId` | `VolumeId` | Empty = any. Otherwise must equal the current volume id, else `EpochMismatch` (a re-created name never accepts the old volume's handles, sessions or tokens — PATH-014, RST-008). |
| `ExpectedVolumeEpoch` | `varuint` | 0 = any. Otherwise must equal the current epoch, else `EpochMismatch`. |
| `Session` | `varuint` | The caller's own session (0 = none). Owns handles, pins and staging sessions created by the request. |
| `Operation` | `OperationId` | Operation id of a mutation, assigned before the first transmission and reused by every retry (OUT-001); empty for reads. |
| `AckWatermark` | `varuint` | All operations of `Session` with a lower sequence are acknowledged by the client and their outcomes may be retired. |
| `Transaction` | `TransactionId?` | Transaction-bound request (§8.4). |
| (extensions) | | Header extension block. |

## 5. Operation identity, idempotency and outcomes (FSD §13)

### 5.1 Rules

* Every mutating volume request carries `Header.Operation` from its first transmission. Ids are
  `(session, sequence)`: sessions are issued by the volume (`OpenSession`, committed), sequences allocated by the
  client, monotonically per session (`ReserveOperationIds` reserves blocks for application-visible ids).
* The leader applies mutations through the operation gate (ARCHITECTURE §5.2). For an id with a retained outcome it
  returns the **stored outcome** (same reply, same `CommitToken`, no second effect), never re-executing against the
  current namespace (OUT-006..009). An in-flight duplicate is coalesced with the original proposal (OUT-005).
* A definite (non-retryable, §11.1) refusal of a request that carries an id (AlreadyExists, FileNotFound, AccessDenied,
  ...) is decided through the log like a success: the leader proposes the request even when its pre-check refuses it,
  apply records the refusal as the id's outcome, and retries, delayed duplicates and `QueryOperation` get that definite
  answer (a refused id never executes later, OUT-013). Retryable refusals answered by the leader's pre-check
  (`SharingViolation`, `WriterConflict`, `PlacementUnavailable` — receipts are per attempt, §5.2 — `NotLeader`) and
  request-validation failures are not recorded: the same id may still execute, so a client that abandons it resolves it
  with `QueryOperation(Resolve: true)`. A refusal decided by apply itself is always the id's outcome.
* Only the session's own principal can retire its outcomes: `Header.AckWatermark` with another principal's session is
  ignored.
* An id whose session expired or whose sequence was retired fails with `OperationExpired` and is never executed as new
  (OUT-012). `QueryOperation(Resolve: true)` fences an id with no outcome (definite `Aborted`) so a delayed copy can
  never commit later (OUT-013).
* An id is bound to the **principal** (another principal's id → `OperationIdMismatch`, no outcome disclosure, OUT-004),
  the **volume and epoch**, the **request kind** and the **payload identity** (§5.2).
* A client that cannot tell whether a transmitted mutation committed reports `DfsCommitOutcomeUnknownException` with
  the id (and transaction id) and resolves it with `QueryOperation`; `QueryOperationResult.ResultFrame` returns the
  original success reply when retained (for example the `HandleId` of a create-open whose reply was lost).
* **Sequence allocation contract.** The leader reserves strictly increasing, non-overlapping blocks per session with
  `ReserveOperationIds`. The SDK uses **only** sequences the leader handed out: it reserves blocks for its own internal
  operations (64 at a time) and single sequences for application-visible ids, so an application id can never collide
  with a sequence the SDK allocated concurrently. A sequence that was allocated but never transmitted is reused by the
  next allocation (so blocks are used up and their reservations retire). An SDK-allocated id whose **only** transmission
  is rejected with `OperationExpired` (or, for requests that refer to no handle, pin or staging session, `LeaseLost` for
  an expired session, or `EpochMismatch` for a re-created volume) did not execute; the SDK opens a new session and
  continues under a fresh id. After more than one transmission such a rejection (`OperationExpired`, `LeaseLost` or
  `EpochMismatch`, for SDK and application ids alike) makes the outcome unknown (`DfsCommitOutcomeUnknownException`),
  because an earlier transmission may have committed before its outcome was retired, its session expired or its volume
  was re-created. An unexpected (non-error) reply type to a transmitted mutation is likewise an unknown outcome.
* **Volume binding of retries.** Every transmission of one operation id carries the same `ExpectedVolumeId` /
  `ExpectedVolumeEpoch`: those of the session the id belongs to (an application id: those of its first transmission).
  Requests about anything a session owns — handles, reader and enumeration pins, staging sessions, transactions, and
  `QueryOperation(Resolve)` of an SDK request's own id — carry the binding of the volume that issued it. A re-created or
  restored volume therefore rejects them with `EpochMismatch` instead of applying them (handle, session and sequence
  numbers restart in a new volume, so an old number may designate something else there); only an SDK-allocated id
  whose single transmission was rejected that way, of a request that refers to nothing session-owned, moves to the new
  volume under a fresh id.
* `Header.AckWatermark` retires outcomes, so the SDK advances it only past sequences whose outcome it has observed and
  will never ask about again. Ids handed to or supplied by the application (`NewOperationIdAsync`,
  `DfsMutationOptions.OperationId`, transaction ids) may be retried or queried by another process after this one is
  gone, so the SDK never advances the watermark past them; their outcomes retire only through the per-session
  retention bounds (OUT-010).

### 5.2 Payload identity

`OperationPayload.ComputeHash(request)` = first 16 bytes of SHA-256 over the request frame re-encoded in identity form
(`OperationPayload.NormalizeRequest`):

* the header is normalized (`Session = 0`, `AckWatermark = 0`, `ExpectedVolumeId = empty`, `ExpectedVolumeEpoch = 0`,
  header extensions removed);
* publications (`CommitFile`, `WriteAll`, `Append`) also drop their **per-attempt upload artifacts**: `Receipts`,
  `Staging` and the uploader-chosen random `Object` id of every `RangeChange`. A client that lost its process (or its
  staged uploads) must upload the content again before retrying with a persisted id — new staging session, new object
  ids, new receipts — and still has to get the stored outcome rather than `OperationIdMismatch` (OUT-002, OUT-006).
  The content stays covered (every change's offset, length, kind, object offset/length, checksum, encoding and inline
  bytes), so SDKs split content into objects deterministically (at `ChunkSize` boundaries, inline when the file fits
  `InlineThreshold`).

The volume name, operation id, transaction id, every other request field and the request's own extensions are
covered. A retry from another process (different session, advanced watermark, re-uploaded content) therefore has the
same identity, while any change of arguments, bytes, path, options or request kind yields a different one
(`OperationIdMismatch`, OUT-003). The server stores this value in the outcome record (`PayloadHash`).

### 5.3 Per-request summary

"Entry" names the volume/control log command (ARCHITECTURE §5.2) the request results in. "Op id": **req** = must
carry an operation id; **opt** = may carry one (then retries are deduplicated); **—** = no operation id.

| Request | Entry | Op id | Retry / duplicate behaviour |
|---|---|---|---|
| `GetClusterMap`, `GetVolumeRoute` | — | — | Read. |
| `JoinCluster` | `AddMember` (control) | — | Idempotent per (node id, name). |
| `RegisterIncarnation` | control registration | — | Idempotent per (node, incarnation). |
| `OpenSession` | `OpenSession` | — | Same `Nonce` → same session while retained; otherwise a spare session that simply expires. |
| `KeepAlive` | `KeepAlive` | — | Naturally idempotent. |
| `QueryOperation` | `FenceOperation` when `Resolve` | — | Idempotent; resolved answers are definite. |
| `ReserveOperationIds` | session reservation (leader-defined) | — | Each call reserves a new block; unused ids expire harmlessly. |
| `OpenHandle` | `OpenHandle` | req for create/truncate (SDK always sends one) | Stored outcome: the same handle. |
| `CloseHandle` | `CloseHandle` | opt | Idempotent by handle id. |
| `ReadRange`, `GetInfo`, `ListDirectory`, `ListSnapshots`, `GetAccessControl`, `GetEffectiveRights`, `ListStreams` | — (ReadIndex) | — | Reads (a `ListDirectory` first page acquires an enumeration pin). |
| `CreateStaging` | `CreateStaging` | req (not for `Renew`) | Stored outcome: the same staging session. |
| `CommitFile`, `WriteAll`, `Append`, `Copy`, `Move`, `Delete`, `CreateDirectory`, `SetPolicy`, `SetAccess`, `SetAccessControl`, `SetOwner` | same name (`SetAcl` for `SetAccess` and `SetAccessControl`) | req | Stored outcome; never applied twice; never re-resolved against the current namespace. |
| `TxBegin` | `TxBegin` | req (becomes the transaction id) | Stored outcome. |
| `TxOp` | leader working root (+ `TxAcquireReservation`) | per inner request | Per inner request. |
| `TxCommit` | `TxCommit` | req | Stored outcome; also resolvable by transaction id. |
| `TxAbort` | `TxAbort` | opt | Idempotent; reports `Committed` when the transaction had committed (abort never reverses a commit). |
| `CreateSnapshot`, `DeleteSnapshot`, `SetRetention` | same name (`SetSnapshotRetention`) | req | Stored outcome. |
| `RestoreSnapshot` | `RestoreBegin`, then `RestoreFinish` (records the outcome) | req | Stored outcome; a retry while the restore runs waits for the same run. |
| `OpenSnapshot` / `CloseSnapshot` | `AcquirePin` / `ReleasePin` | opt / — | With an id: stored outcome (same pin). Close is idempotent. |
| `BackupRegister` (mutating actions), `BackupRequestSync` | volume/control entries | req | Stored outcome. |
| `BackupStatus`, `BackupAwait` | — | — | Reads (await is a bounded wait). |
| `PutObject`, `ReplicateObject` | durable local write | — (object id) | Idempotent per (object id, content); the receipt is bound to the request's staging session (a re-upload under another session durably re-binds the copy first); different content under the same id → `Integrity`. |
| `GetObject`, `LocateObject` | — | — | Reads. |
| `DeleteObjects` | local delete | — | Idempotent; deletes a copy only while its record still carries the decided staging session (`ObjectStagingRef`). |
| `InventoryReport` | — | — (report id, page) | Idempotent per page; the GC answer is only given on the last page. |
| `CopyObjects` | durable local writes | — (from, to) | Idempotent per pair. |
| `RestoreVolume` | `CreateRestoringVolume` / `FinishRestoringVolume` (control) | `RequestId` | Deduplicated by the control group's administrative outcomes. |
| `BackupTargetUpdate` | `RegisterBackupTarget` / `RemoveBackupTarget` (control) | `RequestId` | Deduplicated by the control group's administrative outcomes. |
| `RestoreInstall` | `RestoreChunk` / `RestoreComplete` | — (source tag) | Idempotent per restore source. |
| `RaftBatch`, `RaftSnapshotChunk` | Raft | — | Raft tolerates duplicates/reordering. |
| `AdminRequest` | control/volume entries | `RequestId` | Deduplicated while the leader retains the id; otherwise natural idempotence (for example `AlreadyExists`). |
| `BackupBegin` … `BackupPublish`, `BackupExportRestore` | backup-node local state | — (transfer key / pin) | Resumable and idempotent (BAK-009). |
| `BackupRetention` | backup-node local state | — | Extension is monotonic; queries are reads. |

## 6. Sessions, leases and pins

* A client opens one session per volume (`OpenSession`) lazily and renews it with `KeepAlive` every
  `LeaseDuration / 3`, listing its active handles (lease renewal), live-read pin advances and reader pins
  (snapshot readers and enumerations). The **leader** tracks renewals with its monotonic clock and proposes
  `ExpireHandles`/`ExpireSessions`; client clocks are never authoritative (LOCK-014). A new leader treats every lease
  as renewed at its election.
* `KeepAliveResult.ExpiredHandles` / `ExpiredPins` / `SessionExpired` tell the client which streams to fault
  (`LeaseLost`, `PinExpired`) and when to open a new session (NET-018). A `KeepAlive` rejected with `EpochMismatch`
  (it carries its session's binding) means the volume was re-created or restored: the SDK re-learns the binding at once
  and faults every handle and pin of the old volume.
* Live-read handles report `HandlePinUpdate(handle, pinIndex)` after each read so GC can reclaim objects of older
  revisions; version-pinned handles keep their pin for their lifetime.
* Backup nodes keep sessions and pins locally (no Raft) for snapshot readers (BAK-013, BAK-027).
* Requests that create nothing owned by a session (`GetInfo` by path, `GetVolumeRoute`, `ListSnapshots`,
  `QueryOperation`, `BackupStatus`, `BackupAwait`, `GetAccessControl`, `GetEffectiveRights`, `ListStreams`) may carry
  `Session = 0`; the SDK attaches its session when one is open (for the acknowledgement watermark) but never opens one
  just to read. Requests through handles, pins, staging sessions or transactions carry the owning session.
* `LeaseLost` answers the object a request refers to: for a request through a handle, pin, staging session or
  transaction it means that object is gone (its own lease expired, or its session did), and the SDK faults only what
  depends on it — other handles of the session keep working until `KeepAliveResult` reports them. Only for a request
  that refers to nothing session-owned (for example the first `ListDirectory` page) does `LeaseLost` mean the session
  itself expired, so the SDK opens a new one and retries.
* Whole-file helpers (`WriteAll`, `Append`) share one unbound staging session per client session (`CreateStaging`
  with an empty `Handle`); it stays open across publications (only the leader's own materialization staging closes
  with its publication) and is replaced when the client session expires. Stream writers use a staging session bound
  to their handle; transactions use the staging session returned by `TxBegin`. A publication rejected with `LeaseLost`
  because its helper staging session closed (with the client session) applied nothing: the SDK uploads the content
  again under a new staging session and publishes it under a new SDK-allocated id.

## 7. Data plane

### 7.1 Staging, uploads and receipts

1. The client obtains a staging session and a **staging capability** (`CreateStaging`, or `TxBeginResult` for
   transactions) together with the placement epoch.
2. It computes HRW targets (`Placement.SelectTargets(objectId, placementEpoch, policy, nodes)`, nodes from the cluster
   map: `PlacementEligible`, labels, free bytes) and sends `PutObject` to each target (falling back down the HRW order
   when a target is unavailable). Objects are at most `MaxObjectSize` bytes, logical update units are `ChunkSize`.
3. A storage node verifies the capability (§7.3), the declared length and XxHash128 against the payload and capacity
   admission (`CapacityExhausted` before touching reserves), writes the record, flushes, and only then returns a signed
   `ObjectReceipt` (never before the flush; `SafetyMutations.AcknowledgeBeforeFlush` is a negative control only).
4. The client publishes (`CommitFile`/`WriteAll`/`Append`/transaction requests) with the receipts. The leader validates
   **before proposing**: every receipt MAC; receipt ↔ change binding (`ObjectReceipt.Covers`: object id, checksum,
   object length, encoding; plus volume and staging session); node incarnation equals the registered one; placement
   epoch valid; distinct hosts ≥ `DurableCopiesBeforeAck` and distinct (region, zone) ≥ `DurableZonesBeforeAck` per
   referenced object (copies of the same host count once, REP-005; backups never count). Insufficient receipts →
   `PlacementUnavailable` (the error reply's `Detail` carries the required/achieved copies and zones, surfaced as
   `DfsPlacementUnavailableException.RequiredCopies` etc.); nothing is proposed.

### 7.2 `ObjectReceipt` MAC

`Mac = HMAC-SHA256(ReceiptKey, input)`, `ReceiptKey = HKDF(clusterSecret, "tdfs/v1/receipt")`, where `input` is 121
bytes of fixed-width little-endian fields (`ObjectReceipt.GetMacInput`):

| Offset | Size | Field |
|---|---|---|
| 0 | 16 | ASCII `"tdfs/receipt/v1\0"` |
| 16 | 16 | `Object` (u128) |
| 32 | 16 | `Volume` (guid layout) |
| 48 | 8 | `Staging` (u64) |
| 56 | 16 | `Checksum` (u128) |
| 72 | 8 | `Length` (i64) |
| 80 | 1 | `Encoding` |
| 81 | 16 | `Node` (guid layout) |
| 97 | 8 | `Incarnation` (u64) |
| 105 | 8 | `PlacementEpoch` (u64) |
| 113 | 8 | `DurableAtTicks` (i64) |

The receipt's extension block is not covered by the MAC (it cannot carry trusted information).

### 7.3 `DataCapability`

An opaque token (`bytes`) to clients; storage nodes decode it with `DataCapability.Decode`. Layout: `enum
DataCapabilityKind`, `guid` cluster, `VolumeId`, `varuint` epoch, `string` principal, `StagingId`, `list<ObjectId>`
objects (strictly ascending), `timestamp` expiry, `bytes[0|32]` MAC, extension block.
`Mac = HMAC-SHA256(CapabilityKey, "tdfs/capability/v1\0" ‖ fields-without-MAC ‖ extension block)`,
`CapabilityKey = HKDF(clusterSecret, "tdfs/v1/capability")`; unlike other records, extensions **are** MAC-covered, so a
future issuer can add restrictions as required extensions that old verifiers reject rather than ignore. A verifier
checks: MAC (constant time); cluster; volume and epoch (older epoch → rejected, SEC-007); principal equals the
connection principal; not expired on the verifier's clock; kind matches the request; for `PutObject` the staging id
matches the request; for `GetObject` the object is listed (`AllowsRead`, binary search). Failures → `AccessDenied`.
Node peers may omit capabilities on `GetObject` (repair pulls) — clients never may (SEC-004/005).

### 7.4 Reads

`ReadRange` returns, for one revision, inline bytes or the extents intersecting the covered range plus a read
capability for exactly those objects. The client reads `GetObject` ranges from `ExtentData.Locations` first, then the
HRW targets of `ExtentData.PlacementEpoch`, then a `LocateObject` broadcast. A whole-object read is verified against
`ExtentData.Checksum`; a partial read against `GetObjectResult.UnitChecksums` (CRC32C per `UnitSize` unit, data aligned
to unit boundaries) when the node supplies them, and otherwise by fetching the whole object. A mismatch tries the next
replica; no healthy replica → `DfsIntegrityException`. Holes (ranges inside the file length without extents) read as
zeroes; missing objects never do (STR, DSK-016).

## 8. Operation flows

### 8.1 Publication semantics (`CommitFile`, `WriteAll`, `Append`)

* `RangeChange` kinds: `Data` maps `[Offset, Offset + Length)` to object bytes `[ObjectOffset, ObjectOffset + Length)`
  (`ObjectLength`/`Checksum`/`Encoding` describe the whole object and must match a receipt); `Zero` makes the range a
  hole; `Inline` carries the bytes and is only valid when the resulting file is stored inline (length ≤ the volume's
  `InlineThreshold` and the metadata group's durability satisfies the file's policy — LAY-002). The changes of one request
  must not overlap and must lie inside `[0, NewLength)` (for `Append`: `[0, AppendLength)`); the leader rejects other
  layouts with `InvalidArgument` before proposing (LAY-018).
* `CommitFile`: clip the base revision at `TruncateTo` (when ≥ 0; the lowest length the staged view reached), apply the
  changes, then set `NewLength` (growth creates a hole). This preserves STR-011: truncation followed by extension never
  reveals discarded bytes. `BaseRevision` (non-zero) must equal the inode's revision (`WriterConflict` otherwise).
  Every publication through a handle carries `(Handle, Generation)`; a stale generation or inactive handle is rejected
  with `LeaseLost` (LOCK-04).
* `WriteAll` replaces the whole content (`[0, Length)` described by changes) while preserving the inode;
  `Append` interprets offsets relative to the end of file at apply time and is applied exactly once per id.
* **Representation chosen by the client**: a publication carries `Inline` changes exactly when the resulting stream is
  stored inline — its new length is at most the volume's `InlineThreshold` **and** every majority of the volume's voters
  spans at least `DurableCopiesBeforeAck` distinct hosts and `DurableZonesBeforeAck` distinct zones of the file's
  policy (LAY-002) — and `Data` changes (objects uploaded with receipts meeting the policy) otherwise. Content is split
  into objects at `ChunkSize` boundaries of the file (helpers) or of the dirty range (streams), so retries of the same
  operation carry the same payload identity. `Append` offsets are relative to the end of file; the SDK appends inline
  only when the file's current length plus the appended bytes fit the threshold, and otherwise (or when the leader
  rejects an inline append because the file grew meanwhile, with a new operation id) as objects.
* **Extents → inline**: when a publication whose result is stored inline leaves bytes of objects inside the result —
  base bytes of an extent-based file (for example a truncation to below the threshold followed by small writes), or
  `Data` changes of objects a write-only stream uploaded early and then truncated below the threshold — the leader
  materializes those bytes from the objects into the inline image before proposing; the client never reads the base
  (or its own staged objects) to rewrite it. Staging capabilities never authorize `GetObject`, so the SDK uploads
  staged data early (bounded memory) only for write-only streams; readable writers keep their staged bytes in memory.
* **Inline → extents**: when a publication turns an inline file into an extent-based one, bytes of the new content not
  covered by the change set are taken from the old inline bytes; the leader materializes them as an object written
  under the request's staging session with receipts meeting the file's policy, before proposing. (The client never
  has to read-modify-write to append to a small file.)
* **Extents → inline**: when the result of a publication fits inline (and LAY-002 allows it), the leader reads the kept
  bytes of the old extents back (verified against their checksums) and proposes the content inline; the extents' objects
  become unreferenced at that revision. Either conversion is guarded by the base revision the leader materialized from,
  so a publication that raced with another change of the stream fails with the retryable `WriterConflict` (nothing is
  published) instead of overwriting it.
* Every mutation may request commit-and-snapshot with `SnapshotName`: the snapshot is created in the same log entry
  (FSD §11.2) and returned in the reply.
* Limits (`MaxPublishBytes`, `MaxFileLength`, offsets/lengths non-negative and without overflow — PATH-013) are checked
  before proposing and at apply; violations fail with `LimitExceeded`/`InvalidArgument` and publish nothing.

### 8.2 Opens, handles and sharing

`OpenHandle` performs mode semantics, sharing checks in both directions and writer exclusivity in one entry. Raw enum
values are validated by the server: unknown bits, `FileShare.Inheritable`, a writable handle granting
`FileShare.Write`, `VersionPinned` on a writable handle, and unsupported `FileOptions` → `InvalidArgument`
(`NotSupportedException` on the SDK side for options). Conflicts → `SharingViolation` / `WriterConflict`
(retryable, never waits — LOCK-019). `CloseHandle` releases everything bound to the handle and discards its
unpublished staging.

### 8.3 Reads, enumeration and snapshot views

* Live reads (`ReadRange` through a live handle, `GetInfo`, `ListDirectory`, `GetVolumeRoute(RequireLeader)`) run a
  ReadIndex barrier on the leader.
* `ListDirectory` pins one namespace version for the whole enumeration; the continuation is opaque, binds volume,
  epoch, pin and position, and fails with `ContinuationExpired` once the pin is gone (NS-07, PATH-014).
* Snapshot views: `OpenSnapshot` returns a reader pin; the view resolves paths with `GetInfo`/`ListDirectory`
  (`SnapshotPin` set) and reads with `ReadRange(SnapshotPin, Inode)`. The same flow is served by backup nodes for
  published snapshots, from local state only (BAK-013/014).
* Named streams (Addendum A) are addressed by `file:stream` paths in every path-taking request (`OpenHandle`,
  `GetInfo`, `WriteAll`, `Append`, `Copy`, `Delete`); `ListStreams` enumerates them and `FileInfoData.IsStream` /
  `StreamName` / `StreamCount` describe them. Streams share their file's inode, owner, descriptor and sharing state.
* Access control (Addendum A): `GetAccessControl`, `SetAccessControl`, `SetOwner` and `GetEffectiveRights` read and
  change the inode security descriptor; the volume grant (§8.8) is checked first, then the descriptor. Mutations are
  rechecked against the current descriptor and grants at apply (ACL-05), so a revocation committed before the entry
  wins.

### 8.4 Transactions

`TxBegin` → transaction-bound requests (any volume request with `Header.Transaction` set: opens, reads, `GetInfo`,
`ListDirectory`, `CommitFile` through transaction-bound handles, `WriteAll`, `Append`, `Copy`, `Move`, `Delete`,
`CreateDirectory`, `SetPolicy`, `SetAccess`) → `TxCommit` or `TxAbort`. Transaction-bound requests are applied to the
leader's private working root and recorded in the read/write sets; their replies carry `CommitToken.None` and
revision 0. Writer reservations conflicts fail immediately with `SharingViolation`. Leader change or restart aborts
the transaction (`TransactionAborted`). The unknown outcome of `TxCommit` is resolved with
`QueryOperation(Target = transaction's begin id, IsTransaction = true)`.

### 8.5 Snapshots and restore

`CreateSnapshot` (optionally after `MinimumCommit`), `ListSnapshots`, `OpenSnapshot`/`CloseSnapshot`, `SetRetention`,
`DeleteSnapshot` (retention reference only; `RetentionProtected` while held/unexpired). `RestoreSnapshot` creates a new
volume (new id, epoch 1); the source volume's leader coordinates:

1. It accepts the request with a committed `RestoreBegin` entry (the operation gate checks the request's id; the entry keeps
   the snapshot root reachable even if the snapshot is deleted meanwhile).
2. `RestoreVolume(Create)` to the control leader creates the new volume in the `Restoring` state. A restoring volume is in
   the cluster map but serves no client request (`Unavailable`), so an incomplete volume is never exposed (RST-007).
3. `RestoreInstall(Begin)` to the new volume's leader opens the install and returns its staging session.
4. Every object the snapshot's namespace references is copied to a new id owned by the new volume and bound to that staging
   session: `CopyObjects` on the holders that belong to the new volume's pool (a local copy, no data transfer); an object
   whose copies do not reach its policy's durable copies and zones (for example when the new volume lives in another pool)
   is read, verified and stored on the new volume's placement targets instead. Copy ids are derived deterministically from
   the new volume id and the source id, so retries are idempotent.
5. `RestoreInstall(Stage)` chunks carry the namespace with extents rewritten to the copies; `RestoreInstall(Complete)`
   installs it atomically (one entry) and closes the staging session.
6. `RestoreVolume(Activate)` makes the volume `Active`; a committed `RestoreFinish` entry records the reply under the
   request's operation id (retries get the stored outcome).

Every step is idempotent (deduplicated control request ids, the restore source tag, deterministic copy ids), and a new
leader of the source volume resumes accepted restores from step 2. A restore that fails definitively (for example
`PlacementUnavailable`) abandons its volume (`RestoreVolume(Abandon)`) and records the failure; validation failures
(`SnapshotNotFound`, `AlreadyExists`, `PoolNotFound`, `InvalidPolicy`, `AccessDenied`) are answered before anything is
created or copied.

### 8.6 Backup (late sync, FSD §12)

* **Client → live volume leader**: `BackupRegister` (register/remove/configure/list), `BackupStatus`,
  `BackupRequestSync`, `BackupAwait` (commit-boundary or exact-snapshot barrier; bounded wait; a timeout never rolls
  back the live commit). Registrations live in the volume's control record: the volume leader authorizes the caller,
  wraps the export secret with the cluster key-encryption key and forwards the change to the control leader with
  `BackupTargetUpdate` (request id derived from the volume and the operation id, so retries are deduplicated), then
  waits until its own control replica applied it. Removing a target also releases its transfer pins and drops its
  progress from the volume state. `BackupRequestSync` commits a volume entry that pins the snapshot for the target
  (creating an export snapshot when none is named and the volume changed since the latest one).
* **Exporter (volume leader) → backup node**, authenticated with the target's export credentials:
  `BackupBegin` (transfer key = source cluster, volume, epoch, lineage, snapshot; older epochs rejected — BAK-026;
  `ReseedRequired` when the lineage cannot continue) → `BackupInventory` batches (hint only) → `BackupPutObject` for
  missing objects (verified and durable before acknowledged) → `BackupPutManifest` chunks (opaque manifest owned by the
  backup module, checksummed as a whole) → `BackupPublish`. The target verifies the complete dependency closure
  itself, persists, atomically replaces its published pointer and only then answers `Published = true`; otherwise it
  lists `MissingObjects` / `ManifestIncomplete` and publishes nothing (BAK-004..012). The exporter then records
  progress (`RecordBackupProgress`).
* **Retention** on the backup node is enforced with its own authorization (`BackupRetention`, `SetRetention`,
  `DeleteSnapshot` on the backup node): export credentials can never shorten protected retention (BAK-022).
* **Restore from backup**: `AdminRequest(RestoreFromBackup)` → the control leader creates a new volume and pulls the
  snapshot with `BackupExportRestore` (`Describe` pins the snapshot, then `Manifest` and `Object` chunks, then
  `Release`). Missing key material fails with `RecoveryKeyMissing` (RST-005).

### 8.7 Consensus traffic

Raft messages travel in `RaftBatch` (per destination node, one-way) as `RaftEnvelope(group, RaftWireMessage)`.
**Decision**: Core defines a neutral flat `RaftWireMessage` (etcd `raftpb.Message` style) instead of depending on the
Consensus project; the node runtime maps each `Tedd.Tdfs.Consensus` message record to and from it (field use per
kind is documented on `RaftWireMessage` in §10). This keeps one strict, fuzz-tested codec in Core, lets message
filters and fault injection inspect Raft traffic, and leaves the Consensus core free of wire concerns. Group names are
`"control"` and `"vol:{VolumeId}"` (`ProtocolInfo.VolumeGroup`), keyed by volume id so a re-created volume name never
receives stale traffic. Large snapshots may use `RaftSnapshotChunk` (request/response, flow-controlled) instead of an
`InstallSnapshot` inside a batch. Receivers drop batches whose `From`/`Incarnation` do not match the authenticated
connection, batches of an incarnation older than the registered one, and batches of a cloned process (registered
incarnation, different boot id). A newer, not yet registered incarnation is accepted (§3): a node must take part in the
elections and commits that register its new incarnation.

### 8.8 Administration

`AdminRequest { Op, RequestId, Body }` → `AdminResult { Op, Body }` (failures use `ErrorReply`). Bodies are records
encoded with `WireRecordCodec` (`AdminRequestMessage.Create`/`ReadBody`). Unknown `Op` values decode (raw enum) and are
answered with `Unsupported`.

| `AdminOp` | Request body | Result body | Served by | Permission |
|---|---|---|---|---|
| `ClusterStatus` (1) | `AdminEmptyBody` | `AdminClusterStatusBody` | control leader | ClusterAdmin |
| `ListNodes` (2) | `AdminEmptyBody` | `AdminNodeListBody` | control leader | ClusterAdmin |
| `AddNode` (3) | `AdminAddNodeBody` | `NodeEntry` | control leader | ClusterAdmin |
| `DrainNode` (4) | `AdminNodeNameBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `RemoveNode` (5) | `AdminRemoveNodeBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `SetNodeLabels` (6) | `AdminSetNodeLabelsBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `CreateVolume` (10) | `AdminVolumeSpecBody` | `AdminVolumeInfoBody` | control leader | ClusterAdmin |
| `DeleteVolume` (11) | `AdminVolumeNameBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `ListVolumes` (12) | `AdminEmptyBody` | `AdminNameListBody` | any node | ClusterAdmin |
| `VolumeStatus` (13) | `AdminVolumeNameBody` | `AdminVolumeStatusBody` | volume leader | VolumeAdmin |
| `SetDefaultPolicy` (20) | `AdminSetDefaultPolicyBody` | `AdminEmptyBody` | control leader | VolumeAdmin |
| `DefinePolicy` (21) | `AdminDefinePolicyBody` | `AdminEmptyBody` | control leader | VolumeAdmin |
| `PolicyMigrationStatus` (22) | `AdminVolumeNameBody` | `AdminPolicyMigrationBody` | volume leader | VolumeAdmin |
| `CreatePrincipal` (30) | `AdminCreatePrincipalBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `DeletePrincipal` (31) | `AdminPrincipalNameBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `Grant` / `Revoke` (32/33) | `AdminGrantBody` | `AdminEmptyBody` | control leader | ClusterAdmin or VolumeAdmin |
| `RotateDataKey` (40) | `AdminVolumeNameBody` | `AdminKeyRotationBody` | control leader | VolumeAdmin |
| `ExportRecoveryKey` (41) | `AdminExportRecoveryKeyBody` | `AdminRecoveryKeyBody` | control leader | ClusterAdmin |
| `StartScrub` (50) | `AdminStartScrubBody` | `AdminEmptyBody` | control leader (fans out) | ClusterAdmin |
| `StartRepair` (51) | `AdminVolumeNameBody` | `AdminEmptyBody` | volume leader | VolumeAdmin |
| `ReplicaHealth` (52) | `AdminVolumeNameBody` | `ReplicaHealthData` | volume leader | VolumeAdmin |
| `RestoreFromBackup` (60) | `AdminRestoreFromBackupBody` | `AdminVolumeInfoBody` | control leader | ClusterAdmin |
| `SetFeature` (70) | `AdminSetFeatureBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `AuditLog` (71) | `AdminAuditLogRequestBody` | `AdminNameListBody` | any node | ClusterAdmin |
| `CreateGroup` / `DeleteGroup` (80/81) | `AdminGroupNameBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `AddGroupMember` / `RemoveGroupMember` (82/83) | `AdminGroupMemberBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `ListGroups` (84) | `AdminEmptyBody` | `AdminGroupListBody` | any node | ClusterAdmin |
| `CreatePool` / `SetPoolMembers` (90/91) | `AdminPoolMembersBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `DeletePool` (92) | `AdminPoolNameBody` | `AdminEmptyBody` | control leader | ClusterAdmin |
| `ListPools` (93) | `AdminEmptyBody` | `AdminPoolListBody` | any node | ClusterAdmin |
| `PoolStatus` (94) | `AdminPoolNameBody` | `AdminPoolStatusBody` | any node | ClusterAdmin |

Operations 80–94 belong to Addendum A (groups and storage pools); pool and group changes are audited and pool changes
advance the placement epoch. Volume-scoped operations (`VolumeStatus`, `PolicyMigrationStatus`, `StartScrub`,
`StartRepair`, `ReplicaHealth`, `RestoreFromBackup`) are dispatched by the receiving node to the volume service; a node
without it answers `Unsupported`. Mutating operations are deduplicated by `RequestId` in the control state (the retained
outcome is returned for a retry).

Administrative changes are audited with principal, operation, outcome and scope, never secrets (SEC-016/018).

## 9. Message reference

Each table lists the body fields in wire order (the decoder's order), followed by the terminal extension block.
Types are those of §2. Direction, semantics and errors are in each message's description.

### 9.1 Error

#### `ErrorReply` = 1 (`ErrorReplyMessage`)

Error reply to any request (`MessageType.ErrorReply`). Preserves the error code, operation and transaction identity, retryability and the leader hint across the wire (FMT-011).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Code` | `varint32 (DfsErrorCode, raw)` | Canonical error code (encoded verbatim, including codes unknown to this version). |
| 2 | `Message` | `string` | Human-readable message (never payloads or secrets). |
| 3 | `OperationId` | `OperationId?` | Operation id of the failed request, when known. |
| 4 | `Retryable` | `bool` | True when retrying the same operation may succeed. |
| 5 | `LeaderHint` | `string?` | Endpoint of the current leader, for `DfsErrorCode.NotLeader`. |
| 6 | `TransactionId` | `OperationId? (transaction)` | Transaction id, for transaction failures and unknown transaction outcomes. |
| 7 | `Detail` | `ErrorDetailData?` | Structured details (placement shortfall, exceeded limit, integrity object, adapter phase) recreated on the typed exception; null when the error has none. |
| 8 | — | `extensions` | Terminal extension block. |

### 9.2 Handshake and discovery

#### `Hello` = 10 (`HelloMessage`)

First message on every connection (client → server, node → node). Declares the peer's role, identity, protocol range and features, and contributes a fresh nonce to the authentication transcript. Answered by `HelloChallengeMessage` (or an error reply such as `DfsErrorCode.ClusterMismatch`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ExpectedClusterId` | `guid` | Cluster the peer expects; `Guid.Empty` accepts any cluster. |
| 2 | `ProtocolMin` | `varuint16` | Oldest protocol version the peer speaks. |
| 3 | `ProtocolMax` | `varuint16` | Newest protocol version the peer speaks. |
| 4 | `Features` | `set<string>` | Feature names the peer supports (ordinal set). |
| 5 | `Role` | `enum PeerRole` | Declared role. |
| 6 | `Name` | `string` | Client name or node name (informational for clients; must match the membership for nodes). |
| 7 | `NodeId` | `NodeId?` | Node identity (nodes only). |
| 8 | `Incarnation` | `varuint` | Node incarnation (nodes only; 0 for clients). |
| 9 | `Principal` | `string?` | Principal to authenticate (clients only). |
| 10 | `Nonce` | `bytes[32]` | Fresh random nonce of exactly `ProtocolInfo.NonceSize` bytes. |
| 11 | `BootId` | `u128` | Process boot id of a node peer (0 for clients): random per process start, registered with the incarnation; a process with the registered incarnation but another boot id is a cloned disk and is fenced (`StaleIncarnation`, RSY-021). |
| 12 | — | `extensions` | Terminal extension block. |

#### `HelloChallenge` = 11 (`HelloChallengeMessage`)

Server's answer to `HelloMessage`: cluster identity, the selected protocol version, the server's identity and nonce, and the parameters the peer needs to compute its proof. For client principals the salt and iteration count of the principal's PBKDF2 verifier are returned; for an unknown principal the server returns a deterministic fake salt so principal existence is not disclosed. For nodes the salt is empty and the iteration count is 0 (the proof key is the HKDF-derived node authentication key).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ClusterId` | `guid` | Cluster identity (for a backup node: the source cluster it serves). |
| 2 | `ClusterName` | `string` | Cluster name. |
| 3 | `ProtocolVersion` | `varuint16` | Selected protocol version (the highest version in both ranges). |
| 4 | `Features` | `set<string>` | Features active cluster-wide that the peer also supports. |
| 5 | `ServerName` | `string` | Name of the answering node. |
| 6 | `ServerRole` | `enum PeerRole` | Role of the answering node (`PeerRole.Backup` marks a read-only backup host). |
| 7 | `ServerNodeId` | `NodeId?` | Node identity of the answering node. |
| 8 | `ServerIncarnation` | `varuint` | Current incarnation of the answering node. |
| 9 | `ServerNonce` | `bytes[32]` | Fresh random nonce of exactly `ProtocolInfo.NonceSize` bytes. |
| 10 | `PrincipalSalt` | `bytes` | PBKDF2 salt of the principal's verifier (empty for node peers). |
| 11 | `Pbkdf2Iterations` | `varint32` | PBKDF2-SHA256 iteration count of the verifier (0 for node peers). |
| 12 | — | `extensions` | Terminal extension block. |

#### `HelloProof` = 12 (`HelloProofMessage`)

Peer's proof of knowledge of its secret, computed over the handshake transcript (see `HandshakeAuth` and PROTOCOL.md §3). Answered by `HelloResultMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Proof` | `bytes[32]` | Proof of exactly `ProtocolInfo.MacSize` bytes. |
| 2 | — | `extensions` | Terminal extension block. |

#### `HelloResult` = 13 (`HelloResultMessage`)

Final handshake message. On acceptance it carries the authenticated principal, its cluster-level permissions, the server capabilities and the server's mutual-authentication MAC over the transcript; the peer must verify the MAC before sending any other request. On rejection `Accepted` is false, `Error` explains why (`DfsErrorCode.AuthenticationFailed`, `DfsErrorCode.StaleIncarnation`, ...) and the server closes the connection.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Accepted` | `bool` | True when the peer is authenticated. |
| 2 | `Error` | `enum DfsErrorCode (raw)` | Rejection reason (`DfsErrorCode.None` when accepted). |
| 3 | `Message` | `string` | Human-readable detail (never secrets). |
| 4 | `Principal` | `string` | Authenticated principal (node name for node peers). |
| 5 | `ClusterPermissions` | `enum DfsPermissions (raw)` | Cluster-level permissions of the principal (`DfsPermissions.ClusterAdmin`). |
| 6 | `Capabilities` | `ServerCapabilities?` | Server capabilities and limits (null when rejected). |
| 7 | `ServerMac` | `bytes[0|32]` | Server proof of exactly `ProtocolInfo.MacSize` bytes (empty when rejected). |
| 8 | — | `extensions` | Terminal extension block. |

#### `GetClusterMap` = 14 (`GetClusterMapMessage`)

Requests the cluster map (any authenticated peer → any live or backup node).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `KnownConfigIndex` | `varuint` | Config index of the map the caller already has; when it equals the server's current index the reply omits the map. 0 always returns the map. |
| 2 | — | `extensions` | Terminal extension block. |

#### `ClusterMapResult` = 15 (`ClusterMapResultMessage`)

Reply to `GetClusterMapMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ConfigIndex` | `varuint` | Server's current map config index. |
| 2 | `Map` | `ClusterMapData?` | The map; null when the caller's `GetClusterMapMessage.KnownConfigIndex` is current. |
| 3 | — | `extensions` | Terminal extension block. |

#### `GetVolumeRoute` = 16 (`GetVolumeRouteMessage`)

Requests the route of one volume (any authenticated peer → any live node).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | `RequireLeader` | `bool` | When true the request must be served by the volume leader after a ReadIndex barrier, and the reply carries the volume's last applied commit (used by `DfsVolume.GetInfoAsync`). A non-leader answers `DfsErrorCode.NotLeader` with a leader hint. |
| 3 | — | `extensions` | Terminal extension block. |

#### `VolumeRouteResult` = 17 (`VolumeRouteResultMessage`)

Reply to `GetVolumeRouteMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeEntry` | The volume's map entry. |
| 2 | `Leader` | `NodeId?` | Current (or last known) leader of the volume group. |
| 3 | `LeaderEndpoint` | `string?` | Preferred endpoint of `Leader`. |
| 4 | `Term` | `varuint` | Raft term of the leadership the reply is based on (0 when unknown). |
| 5 | `LastCommit` | `CommitToken` | Last applied commit of the volume (only with `GetVolumeRouteMessage.RequireLeader`; otherwise `CommitToken.None`). |
| 6 | — | `extensions` | Terminal extension block. |

#### `JoinCluster` = 18 (`JoinClusterMessage`)

Seed-join request (joining node → any live node; non-leaders answer `DfsErrorCode.NotLeader` with the control leader hint). The control leader commits `AddMember` and replies after it is applied; the node then becomes a learner of the control group. Idempotent: a repeated join of the same (id, name) returns the current membership. A removed node id is rejected (`DfsErrorCode.AccessDenied`; re-admission needs an explicit administrative join, CON-016).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Node` | `NodeId` | Joining node identity. |
| 2 | `Name` | `string` | Joining node name (unique). |
| 3 | `Labels` | `Labels` | Failure-domain labels. |
| 4 | `Endpoints` | `list<string>` | Listen endpoints. |
| 5 | `Incarnation` | `varuint` | Current incarnation of the joining node. |
| 6 | `CapacityBytes` | `varint` | Configured capacity. |
| 7 | `ProtocolMax` | `varuint16` | Newest protocol version the node speaks (for feature eligibility, FMT-009). |
| 8 | `Features` | `set<string>` | Features the node supports. |
| 9 | `BootId` | `u128` | Process boot id of the joining node (see `HelloMessage.BootId`); registered with the incarnation. |
| 10 | — | `extensions` | Terminal extension block. |

#### `JoinClusterResult` = 19 (`JoinClusterResultMessage`)

Reply to `JoinClusterMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Accepted` | `bool` | True when the node is (now) a member; false when the receiver could not decide (retry at `LeaderEndpoint`). |
| 2 | `Leader` | `NodeId?` | Control leader (hint). |
| 3 | `LeaderEndpoint` | `string?` | Endpoint of the control leader (hint). |
| 4 | `Map` | `ClusterMapData?` | Cluster map including the new member (when accepted). |
| 5 | — | `extensions` | Terminal extension block. |

#### `RegisterIncarnation` = 20 (`RegisterIncarnationMessage`)

Registers a restarted node's new incarnation with the control group (node → control leader) before the node issues receipts (RCV-02). Also updates the node's endpoints (a restarted node may listen on a new port) and reported capacity. Rejected with `DfsErrorCode.StaleIncarnation` when the incarnation is lower than the registered one, or equal to it with a different `BootId` (two processes started from a cloned disk pick the same incarnation; the one registered second is fenced and must stop serving, RSY-021), and with `DfsErrorCode.AccessDenied` for a removed node. Idempotent for the same (node, incarnation, boot id). Raft traffic of a newer, not yet registered incarnation is accepted: committing the registration needs a control quorum, which after a full-cluster restart consists only of not yet registered incarnations.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Node` | `NodeId` | Node identity. |
| 2 | `Incarnation` | `varuint` | New incarnation (greater than the registered one; equal only for a retry with the same boot id). |
| 3 | `Name` | `string` | Node name (must match the membership). |
| 4 | `Labels` | `Labels` | Current labels from the node configuration. |
| 5 | `Endpoints` | `list<string>` | Current listen endpoints. |
| 6 | `CapacityBytes` | `varint` | Configured capacity. |
| 7 | `BootId` | `u128` | Process boot id (see `HelloMessage.BootId`). |
| 8 | — | `extensions` | Terminal extension block. |

#### `RegisterIncarnationResult` = 21 (`RegisterIncarnationResultMessage`)

Reply to `RegisterIncarnationMessage` (sent after the registration is committed and applied).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Accepted` | `bool` | True when the incarnation is registered. |
| 2 | `RegisteredIncarnation` | `varuint` | Incarnation now registered for the node. |
| 3 | `Status` | `enum WireNodeStatus` | Membership status of the node. |
| 4 | `ConfigIndex` | `varuint` | Control config index containing the registration. |
| 5 | `PlacementEpoch` | `varuint` | Current placement epoch. |
| 6 | — | `extensions` | Terminal extension block. |

### 9.3 Sessions and outcomes

#### `OpenSession` = 30 (`OpenSessionMessage`)

Opens a client session on a volume (client → volume leader; committed `OpenSession` entry). Sessions own handles, pins and staging sessions and scope operation-id sequences. `RequestHeader.Session` and `RequestHeader.Operation` are empty. Retries reuse `Nonce`: while the session created for (principal, nonce) is retained the leader returns it instead of creating another one.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (volume addressing only). |
| 2 | `ClientName` | `string` | Informational client name (diagnostics). |
| 3 | `Nonce` | `u128` | Random 128-bit value chosen once per logical open and reused by its retries. |
| 4 | — | `extensions` | Terminal extension block. |

#### `OpenSessionResult` = 31 (`OpenSessionResultMessage`)

Reply to `OpenSessionMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Session` | `varuint` | Server-issued session number (never reused within the volume epoch). |
| 2 | `FirstSequence` | `varuint` | First operation sequence the client may allocate (sequences are monotonic per session). |
| 3 | `LeaseDuration` | `duration` | Handle lease duration; renew with `KeepAlive` every third of it. |
| 4 | `SessionTimeout` | `duration` | Session expires after this long without a `KeepAlive`. |
| 5 | `ServerWallClock` | `timestamp` | Leader's wall clock at apply time (informational; client clocks are never authoritative). |
| 6 | `MaxOutcomesPerSession` | `varint32` | Number of operation outcomes retained per session before the oldest are retired. |
| 7 | `OutcomeRetention` | `duration` | Time an outcome is retained after its session expires. |
| 8 | `Token` | `CommitToken` | Commit of the `OpenSession` entry. |
| 9 | — | `extensions` | Terminal extension block. |

#### `KeepAlive` = 32 (`KeepAliveMessage`)

Renews the session in `RequestHeader.Session`, the leases of its handles and its reader pins (client → volume leader; committed `KeepAlive` entry). The leader tracks renewals with its monotonic clock; client clocks are never authoritative (LOCK-014). Naturally idempotent.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Session` is the session being renewed. |
| 2 | `Handles` | `list<HandleId>` | Handles whose leases are renewed (handles not listed are not renewed). |
| 3 | `PinUpdates` | `list<HandlePinUpdate>` | Pin index advances of live-read handles. |
| 4 | `ReaderPins` | `list<varuint>` | Snapshot-reader and enumeration pins whose lifetime is renewed. |
| 5 | — | `extensions` | Terminal extension block. |

#### `KeepAliveResult` = 33 (`KeepAliveResultMessage`)

Reply to `KeepAliveMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ExpiredHandles` | `list<HandleId>` | Listed handles that are no longer active (expired or closed): their streams must fault with `DfsErrorCode.LeaseLost`. |
| 2 | `ExpiredPins` | `list<varuint>` | Listed reader pins that are no longer held (`DfsErrorCode.PinExpired`). |
| 3 | `SessionExpired` | `bool` | True when the session itself has expired: every handle, pin and staging session is gone and the client must open a new session. |
| 4 | `LeaseDuration` | `duration` | Current lease duration. |
| 5 | `ServerWallClock` | `timestamp` | Leader's wall clock (informational). |
| 6 | — | `extensions` | Terminal extension block. |

#### `QueryOperation` = 34 (`QueryOperationMessage`)

Queries the durable outcome of an operation id or transaction (client → volume leader; ReadIndex read, or a committed `FenceOperation` entry when `Resolve` is set). Only the principal that owns the operation may query it (OUT-004).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (`RequestHeader.Operation` empty). |
| 2 | `Target` | `OperationId` | Operation id to query; for a transaction, the operation id wrapped by its `DfsTransactionId`. |
| 3 | `IsTransaction` | `bool` | True to query the final state of the transaction `Target` began. |
| 4 | `Resolve` | `bool` | True to make the answer definite: an id with no outcome is fenced (recorded as aborted) so a delayed copy can never commit later (OUT-013). Never returns `WireOutcomeStatus.NotFound` or `WireOutcomeStatus.Pending`. |
| 5 | — | `extensions` | Terminal extension block. |

#### `QueryOperationResult` = 35 (`QueryOperationResultMessage`)

Reply to `QueryOperationMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Target` | `OperationId` | The queried operation id. |
| 2 | `Status` | `enum WireOutcomeStatus` | Outcome status. |
| 3 | `Token` | `CommitToken` | Commit token of a committed operation (`CommitToken.None` otherwise). |
| 4 | `Error` | `enum DfsErrorCode (raw)` | For a completed-with-failure or aborted operation, its error code (`DfsErrorCode.None` for success). |
| 5 | `Detail` | `string?` | Human-readable detail. |
| 6 | `Operation` | `enum DfsOperationKind (raw)` | Kind of the original operation, when known. |
| 7 | `ResultFrame` | `bytes` | Encoded success reply frame of the original request (for example an `OpenHandleResult`), when the outcome record retains it; empty otherwise. |
| 8 | — | `extensions` | Terminal extension block. |

#### `ReserveOperationIds` = 36 (`ReserveOperationIdsMessage`)

Reserves a block of operation sequences in the caller's session (client → volume leader) so that ids handed to the application (`DfsVolume.NewOperationIdAsync`) are known to the leader and not retired before use.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Session` is the session to reserve in. |
| 2 | `Count` | `varint32` | Number of sequences to reserve (1..65536). |
| 3 | — | `extensions` | Terminal extension block. |

#### `ReserveOperationIdsResult` = 37 (`ReserveOperationIdsResultMessage`)

Reply to `ReserveOperationIdsMessage`: sequences `[FirstSequence, FirstSequence + Count)` of `Session`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Session` | `varuint` | Session the sequences belong to. |
| 2 | `FirstSequence` | `varuint` | First reserved sequence. |
| 3 | `Count` | `varint32` | Number of reserved sequences. |
| 4 | — | `extensions` | Terminal extension block. |

### 9.4 Namespace and files

#### `OpenHandle` = 40 (`OpenHandleMessage`)

Opens a handle on a file (client → volume leader; one committed `OpenHandle` entry). Performs path resolution, permission check, `FileMode` semantics (create/truncate), sharing checks against every active handle in both directions, writer exclusivity, and allocates the handle with a lease (and, for version-pinned reads, a pin). Writable opens increment the inode writer generation. Creating/truncating opens must carry an operation id; retries return the stored outcome. Transaction-bound opens (header transaction set) are private to the transaction. Errors: FileNotFound, DirectoryNotFound, AlreadyExists, IsADirectory, AccessDenied, SharingViolation, WriterConflict, InvalidArgument (unknown mode/access/share/options bits, Inheritable, writable handle granting FileShare.Write, VersionPinned on a writable handle), InvalidPath, LimitExceeded (handles per session).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | Path to open (re-validated and NFC-normalized by the server). |
| 3 | `Mode` | `enum FileMode (raw)` | Open mode (validated by the server). |
| 4 | `Access` | `enum FileAccess (raw)` | Requested access (validated by the server). |
| 5 | `Share` | `enum FileShare (raw)` | Sharing granted to other handles (validated by the server). |
| 6 | `Options` | `enum FileOptions (raw)` | File options; anything but None/Asynchronous/SequentialScan/RandomAccess is rejected. |
| 7 | `VersionPinned` | `bool` | True for a version-pinned read handle: all reads use the revision current at open. |
| 8 | `WriteThrough` | `bool` | Informational: the client publishes on every write. |
| 9 | `StoragePolicy` | `string?` | Named storage policy for a newly created file; null = the volume default. |
| 10 | — | `extensions` | Terminal extension block. |

#### `OpenHandleResult` = 41 (`OpenHandleResultMessage`)

Reply to `OpenHandleMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Handle` | `HandleId` | The new handle. |
| 2 | `Info` | `FileInfoData` | Metadata of the opened file at open time (inode, revision, length, ...). |
| 3 | `WriterGeneration` | `varuint` | Writer generation bound to the handle (0 for read-only handles); every publication through the handle must carry it. |
| 4 | `PinIndex` | `varuint` | GC-protection index of the handle (the revision pinned for version-pinned handles). |
| 5 | `LeaseDuration` | `duration` | Lease duration; renew with `KeepAlive`. |
| 6 | `Created` | `bool` | True when the open created the file. |
| 7 | `Truncated` | `bool` | True when the open truncated the file. |
| 8 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 9 | — | `extensions` | Terminal extension block. |

#### `CloseHandle` = 42 (`CloseHandleMessage`)

Closes a handle (client → volume leader; committed `CloseHandle` entry): releases its sharing reservation, writer authority, pin and bound staging session. Staged but unpublished data is discarded. Idempotent: closing an unknown or already closed handle succeeds with `WasOpen = false`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Handle` | `HandleId` | Handle to close. |
| 3 | `Generation` | `varuint` | Writer generation of the handle (0 for read-only handles); a mismatch is ignored for closing but reported via `WasOpen = false`. |
| 4 | — | `extensions` | Terminal extension block. |

#### `CloseHandleResult` = 43 (`CloseHandleResultMessage`)

Reply to `CloseHandleMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `WasOpen` | `bool` | True when the handle was active and is now closed. |
| 2 | `Token` | `CommitToken` | Commit of the close entry (`CommitToken.None` when nothing was proposed). |
| 3 | — | `extensions` | Terminal extension block. |

#### `ReadRange` = 44 (`ReadRangeMessage`)

Resolves the committed bytes of a file range (client → volume leader, or backup node for snapshots). Exactly one addressing mode: a handle (live reads run a ReadIndex barrier; version-pinned and transaction-bound handles read their fixed view), or `(SnapshotPin, Inode)` for snapshot views. Live reads by inode without a handle are rejected (they would bypass sharing). One call uses exactly one revision (STR-014). The reply carries inline bytes or the extents covering the range plus a read capability for those objects; the client fetches object bytes from storage nodes and verifies checksums. Errors: InvalidHandle, LeaseLost, PinExpired, InvalidArgument (negative offset/length, offset + length overflow), Integrity.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Handle` | `HandleId` | Handle to read through (empty when reading a snapshot by inode). |
| 3 | `SnapshotPin` | `varuint` | Snapshot reader pin (from `OpenSnapshot`); 0 = none. |
| 4 | `Inode` | `InodeId` | Inode to read when addressing by snapshot pin; empty otherwise. |
| 5 | `Offset` | `varint` | First byte to read. |
| 6 | `Length` | `varint` | Number of bytes requested. |
| 7 | `Stream` | `string?` | Named stream to read when addressing a snapshot by `(SnapshotPin, Inode)` (Addendum A, ADS-020); null = the default stream. Must be null for handle reads (a handle is bound to its stream). |
| 8 | — | `extensions` | Terminal extension block. |

#### `ReadRangeResult` = 45 (`ReadRangeResultMessage`)

Reply to `ReadRangeMessage`. The covered range is `[Offset, Offset + Length)`, clipped to the file length and possibly shorter than requested (bounded extent count); a zero `Length` at `Offset` >= `FileLength` is end of file. Bytes of the covered range not covered by an extent are holes (zeroes).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Revision` | `varuint` | Revision the read was served from. |
| 2 | `FileLength` | `varint` | File length at that revision. |
| 3 | `Offset` | `varint` | Start of the covered range (equals the requested offset). |
| 4 | `Length` | `varint` | Length of the covered range. |
| 5 | `IsInline` | `bool` | True when the file is stored inline; `InlineData` then holds the covered bytes. |
| 6 | `InlineData` | `bytes` | Covered bytes of an inline file (empty otherwise). |
| 7 | `Extents` | `list<ExtentData>` | Extents intersecting the covered range, in ascending offset order (whole extents; the client clips). |
| 8 | `ReadCapability` | `bytes` | Encoded `DataCapability` allowing `GetObject` on exactly the listed objects (empty for inline files). |
| 9 | `CapabilityExpiry` | `timestamp` | Expiry of the read capability. |
| 10 | `ObservedAt` | `CommitToken` | Commit position the read observed (for live reads: at or after the ReadIndex point). |
| 11 | — | `extensions` | Terminal extension block. |

#### `GetInfo` = 46 (`GetInfoMessage`)

Returns metadata of a path, of the inode behind a handle, or of a path inside a snapshot (client → volume leader or backup node; ReadIndex read). Transaction-bound requests read the transaction view and record the read (or the absent name) for conflict validation. Absence is not an error: `Exists = false` (FSD §5.4); authorization, availability and integrity failures are errors.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Path` | `string` | Path to describe (ignored when `Handle` is set). |
| 3 | `Handle` | `HandleId` | Handle whose inode to describe (live view for live handles, the pinned revision for version-pinned handles); empty = by path. |
| 4 | `SnapshotPin` | `varuint` | Resolve `Path` inside the snapshot held by this reader pin; 0 = live (or transaction) view. |
| 5 | — | `extensions` | Terminal extension block. |

#### `GetInfoResult` = 47 (`GetInfoResultMessage`)

Reply to `GetInfoMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Exists` | `bool` | False only for genuine absence (missing entry or missing/non-directory ancestor). |
| 2 | `Info` | `FileInfoData?` | Metadata when the entry exists. |
| 3 | `ObservedAt` | `CommitToken` | Commit position the answer is based on. |
| 4 | — | `extensions` | Terminal extension block. |

#### `ListDirectory` = 48 (`ListDirectoryMessage`)

Lists one page of a directory at a pinned namespace version (client → volume leader or backup node). The first page pins the directory version (an enumeration pin, renewed by `KeepAlive` and by subsequent pages); the opaque continuation binds volume, epoch, pin and position, and an expired or foreign continuation fails with `DfsErrorCode.ContinuationExpired` instead of restarting against a newer version (NS-07). Transaction-bound listings record the enumeration predicate. Errors: DirectoryNotFound, NotADirectory, ContinuationExpired, InvalidArgument (page size outside 1..10000, empty kinds, malformed pattern).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Path` | `string` | Directory path. |
| 3 | `Pattern` | `string` | Name pattern with `*` and `?` wildcards (ordinal). |
| 4 | `Kinds` | `enum WireEntryKinds (raw)` | Entry kinds to return. |
| 5 | `PageSize` | `varint32` | Maximum entries in the page (1..10000). |
| 6 | `Continuation` | `string?` | Continuation from the previous page; null for the first page. |
| 7 | `SnapshotPin` | `varuint` | List inside the snapshot held by this reader pin; 0 = live (or transaction) view. |
| 8 | — | `extensions` | Terminal extension block. |

#### `ListDirectoryResult` = 49 (`ListDirectoryResultMessage`)

Reply to `ListDirectoryMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Entries` | `list<DirectoryEntryData>` | Entries of the page in ascending ordinal name order. |
| 2 | `Continuation` | `string?` | Continuation for the next page; null when the listing is complete. |
| 3 | `PinnedAt` | `CommitToken` | Namespace version the whole listing is pinned to. |
| 4 | `EnumerationPin` | `varuint` | Enumeration pin holding that version (renew it with `KeepAlive` reader pins, release it early with `CloseSnapshot`); 0 when the listing completed in this page. |
| 5 | `Directory` | `InodeId` | Inode of the listed directory. |
| 6 | `DirectoryRevision` | `varuint` | Revision of the listed directory at the pinned version. |
| 7 | — | `extensions` | Terminal extension block. |

#### `CreateStaging` = 50 (`CreateStagingMessage`)

Opens a staging session for uploads (client → volume leader; committed `CreateStaging` entry) and returns a staging capability for `PutObject`. A session bound to a handle closes with the handle; an unbound session belongs to the caller's session (and is closed when it expires). Setting `Renew` re-issues a capability for an existing open session without a new entry. A closed staging session is the authoritative decision that its unreferenced objects are garbage (ARCHITECTURE §8).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Handle` | `HandleId` | Writable handle to bind the session to; empty = bound to the caller's session. |
| 3 | `Renew` | `StagingId` | Existing open staging session whose capability is renewed; empty = create a new session. |
| 4 | — | `extensions` | Terminal extension block. |

#### `CreateStagingResult` = 51 (`CreateStagingResultMessage`)

Reply to `CreateStagingMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Staging` | `StagingId` | Staging session id. |
| 2 | `Capability` | `bytes` | Encoded staging `DataCapability`. |
| 3 | `CapabilityExpiry` | `timestamp` | Capability expiry (renew before it). |
| 4 | `PlacementEpoch` | `varuint` | Placement epoch to place uploads under. |
| 5 | `Token` | `CommitToken` | Commit of the staging entry (`CommitToken.None` for a renewal). |
| 6 | — | `extensions` | Terminal extension block. |

#### `CommitFile` = 52 (`CommitFileMessage`)

Publishes one complete new revision of a file through a writable handle (client → volume leader; committed `CommitFile` entry, FSD §10.3). Before proposing, the leader validates the handle and writer generation (LOCK-04), authorization, base revision, the receipts (MAC, binding, incarnation, placement epoch, durable copies/zones per policy) and limits; insufficient receipts fail with PlacementUnavailable and nothing is proposed. Apply order: clip at `TruncateTo` (when ≥ 0), apply `Changes` in order, set the length to `NewLength` (extension creates a hole). When the result turns an inline file into an extent-based one, bytes not covered by changes are taken from the old inline content. Transaction-bound commits stage into the transaction. Errors: LeaseLost, InvalidHandle, WriterConflict (base revision), PlacementUnavailable, LimitExceeded, InvalidArgument, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Handle` | `HandleId` | Writable handle. |
| 3 | `Generation` | `varuint` | Writer generation bound to the handle. |
| 4 | `BaseRevision` | `varuint` | Revision the staged changes were made against (the `Revision` reported by the open or the last publication); the commit fails with WriterConflict when the stream's content changed after it (metadata changes such as ACL or rename do not conflict); 0 = do not check. |
| 5 | `TruncateTo` | `varint` | Lowest length the staged view reached since the base revision (−1 = no truncation); content beyond it is discarded before changes apply, so extension never reveals discarded bytes (STR-011). |
| 6 | `NewLength` | `varint` | File length after publication. |
| 7 | `Changes` | `list<RangeChange>` | Changed ranges with absolute offsets, applied in order. |
| 8 | `Receipts` | `list<ObjectReceipt>` | Durability receipts for every object referenced by a Data change. |
| 9 | `Staging` | `StagingId` | Staging session the referenced objects were uploaded under. |
| 10 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 11 | — | `extensions` | Terminal extension block. |

#### `CommitFileResult` = 53 (`CommitFileResultMessage`)

Reply to `CommitFileMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode published. |
| 2 | `Revision` | `varuint` | New revision (0 when transaction-bound). |
| 3 | `NewLength` | `varint` | File length after publication. |
| 4 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 5 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 6 | — | `extensions` | Terminal extension block. |

#### `WriteAll` = 54 (`WriteAllMessage`)

Atomically replaces the complete contents of a file by path, creating it if absent, preserving the inode of an existing file (client → volume leader; one committed `WriteAll` entry with a short implicit writer reservation). The new content is `[0, Length)` described by `Changes` (uncovered ranges are holes); small files use one Inline change. Receipts are validated as for `CommitFile`. Errors: SharingViolation/WriterConflict (another writer holds the inode), IsADirectory, DirectoryNotFound (parent missing and not `CreateParents`), PlacementUnavailable, LimitExceeded, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | Destination path. |
| 3 | `Length` | `varint` | New file length. |
| 4 | `Changes` | `list<RangeChange>` | Content ranges (absolute offsets). |
| 5 | `Receipts` | `list<ObjectReceipt>` | Durability receipts for every referenced object. |
| 6 | `Staging` | `StagingId` | Staging session of the uploads (empty when all content is inline). |
| 7 | `StoragePolicy` | `string?` | Named storage policy when the file is created; null = volume default. |
| 8 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 9 | `CreateParents` | `bool` | True to create missing parent directories. |
| 10 | `ExpectedRevision` | `varuint` | Replace only if the current revision (`FileInfoData.Revision` of `Path`) equals this value (0 = unconditional); mismatch, or an absent file or stream, → WriterConflict (a conditional write never creates). |
| 11 | — | `extensions` | Terminal extension block. |

#### `WriteAllResult` = 55 (`WriteAllResultMessage`)

Reply to `WriteAllMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode written (preserved when the file existed). |
| 2 | `Revision` | `varuint` | New revision (0 when transaction-bound). |
| 3 | `Length` | `varint` | New length. |
| 4 | `Created` | `bool` | True when the file was created. |
| 5 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 6 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 7 | — | `extensions` | Terminal extension block. |

#### `Append` = 56 (`AppendMessage`)

Atomically appends one complete suffix under one operation id (client → volume leader; committed `Append` entry). Change offsets are relative to the end of file at apply time, so the client needs no prior read; a retry returns the stored outcome and never appends twice (OUT-006). Optionally through a writable handle (then the generation is checked). Errors as for `WriteAll`, plus FileNotFound when absent and not `CreateIfMissing`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | File path (ignored when `Handle` is set). |
| 3 | `Handle` | `HandleId` | Writable handle to append through; empty = by path with an implicit writer reservation. |
| 4 | `Generation` | `varuint` | Writer generation of `Handle` (0 when appending by path). |
| 5 | `AppendLength` | `varint` | Number of bytes appended (the suffix is `[oldEnd, oldEnd + AppendLength)`). |
| 6 | `Changes` | `list<RangeChange>` | Suffix ranges with offsets relative to the old end of file. |
| 7 | `Receipts` | `list<ObjectReceipt>` | Durability receipts for every referenced object. |
| 8 | `Staging` | `StagingId` | Staging session of the uploads (empty when all content is inline). |
| 9 | `CreateIfMissing` | `bool` | True to create the file when absent (File.AppendAllBytes semantics). |
| 10 | `StoragePolicy` | `string?` | Named storage policy when the file is created; null = volume default. |
| 11 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 12 | — | `extensions` | Terminal extension block. |

#### `AppendResult` = 57 (`AppendResultMessage`)

Reply to `AppendMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode appended to. |
| 2 | `Revision` | `varuint` | New revision (0 when transaction-bound). |
| 3 | `OldLength` | `varint` | Length before the append (where the suffix starts). |
| 4 | `NewLength` | `varint` | Length after the append. |
| 5 | `Created` | `bool` | True when the file was created. |
| 6 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 7 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 8 | — | `extensions` | Terminal extension block. |

#### `Copy` = 58 (`CopyMessage`)

Copies a file inside a volume as metadata only (client → volume leader; one committed `Copy` entry): the destination receives the source revision's extent map (objects are shared) or inline bytes; inline streams are first stored as objects by the leader when the destination's policy is stronger than what inline metadata guarantees (LAY-002). The source revision is pinned for the whole operation; an existing destination keeps its inode identity. Streams (Addendum A, ADS-04): a plain source copied to a new plain destination copies the default stream and every named stream; otherwise exactly one stream is copied — the addressed source stream (default stream for a plain path) replaces or creates the addressed destination stream (default stream for a plain path) — and the destination's other streams are kept (a source's named streams are never merged into an existing file). `Overwrite` is needed only when the addressed destination stream exists. Errors: FileNotFound (source file or stream), AlreadyExists (destination stream exists and not `Overwrite`), InvalidArgument (copy onto the source's own file), SharingViolation, IsADirectory, CrossVolume, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Source` | `string` | Source file path. |
| 3 | `Destination` | `string` | Destination file path. |
| 4 | `Overwrite` | `bool` | True to replace an existing destination file. |
| 5 | `ExpectedSourceRevision` | `varuint` | Copy only if the source (`FileInfoData.Revision` of `Source`) is at this revision (0 = current); mismatch → WriterConflict. |
| 6 | `StoragePolicy` | `string?` | Named storage policy for a newly created destination; null = the source policy. |
| 7 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 8 | — | `extensions` | Terminal extension block. |

#### `CopyResult` = 59 (`CopyResultMessage`)

Reply to `CopyMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Destination inode. |
| 2 | `Revision` | `varuint` | Destination revision (0 when transaction-bound). |
| 3 | `SourceRevision` | `varuint` | Source revision that was copied. |
| 4 | `Created` | `bool` | True when the destination was created. |
| 5 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 6 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 7 | — | `extensions` | Terminal extension block. |

#### `Move` = 60 (`MoveMessage`)

Atomically renames a file or directory inside a volume (client → volume leader; committed `Move` entry). Handles keep addressing the inode (NS-06). Replacing an existing destination with open handles requires all of them to grant FileShare.Delete; the replaced inode is detached. Errors: FileNotFound/DirectoryNotFound (source kind mismatch or absent), AlreadyExists, SharingViolation, InvalidArgument (root, move into own subtree), CrossVolume, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Source` | `string` | Source path. |
| 3 | `Destination` | `string` | Destination path. |
| 4 | `Overwrite` | `bool` | True to replace an existing destination file (directories are never replaced). |
| 5 | `ExpectDirectory` | `bool` | True when the source must be a directory (DfsDirectoryClient); false when it must be a file. |
| 6 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 7 | — | `extensions` | Terminal extension block. |

#### `MoveResult` = 61 (`MoveResultMessage`)

Reply to `MoveMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode moved. |
| 2 | `Revision` | `varuint` | Revision of the moved inode (0 when transaction-bound). |
| 3 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 4 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 5 | — | `extensions` | Terminal extension block. |

#### `Delete` = 62 (`DeleteMessage`)

Deletes a file or directory (client → volume leader; committed `Delete` entry). Deleting an absent file succeeds (`Existed = false`); an absent directory fails with DirectoryNotFound. A non-empty directory requires `Recursive`, which atomically detaches the whole subtree. Files with open handles require FileShare.Delete on all of them; the inode stays readable through those handles until they close (OPEN-017). Errors: SharingViolation, DirectoryNotEmpty, InvalidArgument (root), AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | Path to delete. |
| 3 | `Recursive` | `bool` | True to delete a non-empty directory with its subtree. |
| 4 | `ExpectDirectory` | `bool` | True when the entry must be a directory; false when it must be a file. |
| 5 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 6 | — | `extensions` | Terminal extension block. |

#### `DeleteResult` = 63 (`DeleteResultMessage`)

Reply to `DeleteMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Existed` | `bool` | True when an entry was deleted. |
| 2 | `Inode` | `InodeId` | Inode deleted (empty when nothing existed). |
| 3 | `Token` | `CommitToken` | Commit of the delete (`CommitToken.None` when nothing existed or when transaction-bound). |
| 4 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 5 | — | `extensions` | Terminal extension block. |

#### `CreateDirectory` = 64 (`CreateDirectoryMessage`)

Creates a directory (client → volume leader; committed `CreateDirectory` entry). Idempotent: an existing directory succeeds with `Created = false`. Errors: AlreadyExists (a file is in the way), DirectoryNotFound (parent missing and not `Recursive`), NotADirectory, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | Directory path. |
| 3 | `Recursive` | `bool` | True to create missing ancestors. |
| 4 | `SnapshotName` | `string?` | Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none. |
| 5 | — | `extensions` | Terminal extension block. |

#### `CreateDirectoryResult` = 65 (`CreateDirectoryResultMessage`)

Reply to `CreateDirectoryMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Info` | `FileInfoData` | Metadata of the (new or existing) directory. |
| 2 | `Created` | `bool` | True when the directory (or an ancestor) was created. |
| 3 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 4 | `Snapshot` | `SnapshotInfoData?` | The snapshot created by commit-and-snapshot, or null. |
| 5 | — | `extensions` | Terminal extension block. |

#### `SetPolicy` = 66 (`SetPolicyMessage`)

Selects a named storage policy for an inode (client → volume leader; committed `SetPolicy` entry). Policies attach to inode identity. Inline streams of the inode are first stored as objects by the leader when the new policy is stronger than what inline metadata guarantees (LAY-002). The change is not reported effective before migration of existing objects completes (FSD §9.2). Errors: InvalidPolicy (unknown name), FileNotFound, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | File or directory path. |
| 3 | `PolicyName` | `string` | Named policy of the volume (empty = the volume default). |
| 4 | — | `extensions` | Terminal extension block. |

#### `SetPolicyResult` = 67 (`SetPolicyResultMessage`)

Reply to `SetPolicyMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode changed. |
| 2 | `Revision` | `varuint` | New revision (0 when transaction-bound). |
| 3 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 4 | — | `extensions` | Terminal extension block. |

#### `SetAccess` = 68 (`SetAccessMessage`)

Changes an inode's access flags (client → volume leader; committed `SetAcl` entry). Only the owner or a volume administrator may change them. Errors: FileNotFound, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | File or directory path. |
| 3 | `OwnerOnly` | `bool` | Only the owner may access the inode. |
| 4 | `ReadOnlyForOthers` | `bool` | Principals other than the owner may only read. |
| 5 | — | `extensions` | Terminal extension block. |

#### `SetAccessResult` = 69 (`SetAccessResultMessage`)

Reply to `SetAccessMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode changed. |
| 2 | `Revision` | `varuint` | New revision (0 when transaction-bound). |
| 3 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound: nothing is committed before `TxCommit`). |
| 4 | — | `extensions` | Terminal extension block. |

#### `GetAccessControl` = 70 (`GetAccessControlMessage`)

Reads the security descriptor of a file, directory or stream path (client → volume leader or backup node; ReadIndex read; Addendum A, ACL-02/03). The reply carries the owner, the protection flag, the explicit entries in canonical order and the inherited entries generation by generation (`IsInherited = true`, with `InheritedFrom`), exactly as `DfsAccessEvaluator.EffectiveRules` computes them. A stream path reads its file's descriptor. Requires the volume Read grant and ReadPermissions. Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidPath, PinExpired (snapshot view).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Path` | `string` | Path whose descriptor is read (re-validated and NFC-normalized by the server). |
| 3 | `SnapshotPin` | `varuint` | Read the descriptor inside the snapshot held by this reader pin; 0 = live (or transaction) view. |
| 4 | — | `extensions` | Terminal extension block. |

#### `GetAccessControlResult` = 71 (`GetAccessControlResultMessage`)

Reply to `GetAccessControlMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Descriptor` | `SecurityDescriptor` | The full descriptor: owner, protection flag, explicit then inherited entries in evaluation order. |
| 2 | `Inode` | `InodeId` | Inode the descriptor belongs to. |
| 3 | `Revision` | `varuint` | Revision of the inode at the observed position. |
| 4 | `ObservedAt` | `CommitToken` | Commit position the answer is based on. |
| 5 | — | `extensions` | Terminal extension block. |

#### `SetAccessControl` = 72 (`SetAccessControlMessage`)

Atomically replaces the explicit entries and the protection flag of an inode (client → volume leader; committed `SetAcl` entry; Addendum A, ACL-07). Inherited entries and the owner in `Descriptor` are ignored (the owner changes only through `SetOwner`). Requires the volume Write grant and ChangePermissions; the change bumps the inode revision. A stream path changes its file's descriptor. Retries reuse the operation id and receive the stored outcome. Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidArgument (invalid explicit entries), InvalidPath.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | File, directory or stream path. |
| 3 | `Descriptor` | `SecurityDescriptor` | New explicit entries and protection flag (validated by the server). |
| 4 | — | `extensions` | Terminal extension block. |

#### `SetAccessControlResult` = 73 (`SetAccessControlResultMessage`)

Reply to `SetAccessControlMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode changed. |
| 2 | `Revision` | `varuint` | New revision of the inode (0 when transaction-bound). |
| 3 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound). |
| 4 | — | `extensions` | Terminal extension block. |

#### `SetOwner` = 74 (`SetOwnerMessage`)

Changes the owner of an inode (client → volume leader; committed `SetOwner` entry; Addendum A, ACL-05/014): a caller holding TakeOwnership may make itself the owner; members of Administrators may set any owner. `CreatorOwner` is rejected (InvalidArgument). Requires the volume Write grant; the change bumps the inode revision. A stream path changes its file's owner. Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidArgument, InvalidPath.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the mutation (retries reuse it). |
| 2 | `Path` | `string` | File, directory or stream path. |
| 3 | `Owner` | `Identity` | New owner identity. |
| 4 | — | `extensions` | Terminal extension block. |

#### `SetOwnerResult` = 75 (`SetOwnerResultMessage`)

Reply to `SetOwnerMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Inode` | `InodeId` | Inode changed. |
| 2 | `Revision` | `varuint` | New revision of the inode (0 when transaction-bound). |
| 3 | `Token` | `CommitToken` | Commit token of the decision (`CommitToken.None` when transaction-bound). |
| 4 | — | `extensions` | Terminal extension block. |

#### `GetEffectiveRights` = 76 (`GetEffectiveRightsMessage`)

The rights an identity would be granted on a path (client → volume leader or backup node; ReadIndex read; Addendum A, ACL-034): `DfsAccessEvaluator.GrantedRights` over the effective entries, with the identity's groups and administrator flag resolved from the identity database (volume grants are not folded in). Querying another identity requires ReadPermissions on the path; querying oneself (`Identity` null) only the volume Read grant. Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidPath.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Path` | `string` | File, directory or stream path (a stream path evaluates its file). |
| 3 | `Identity` | `Identity?` | Identity to evaluate (`bool` presence, then the identity); null = the caller. |
| 4 | `SnapshotPin` | `varuint` | Evaluate inside the snapshot held by this reader pin; 0 = live view. |
| 5 | — | `extensions` | Terminal extension block. |

#### `GetEffectiveRightsResult` = 77 (`GetEffectiveRightsResultMessage`)

Reply to `GetEffectiveRightsMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Rights` | `varuint` | Granted rights (raw `DfsFileSystemRights` bits; values above 32 bits are malformed). |
| 2 | `ObservedAt` | `CommitToken` | Commit position the answer is based on. |
| 3 | — | `extensions` | Terminal extension block. |

#### `ListStreams` = 78 (`ListStreamsMessage`)

Lists the named streams of a file or directory (client → volume leader or backup node; ReadIndex read; Addendum A, ADS-03/005): name, length and revision of every named stream (not the default stream), ordinal by name. Requires the volume Read grant and ReadAttributes on the inode (or ListDirectory on its parent). Errors: FileNotFound (absent path), InvalidPath (a stream path), AccessDenied, PinExpired (snapshot view).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Path` | `string` | File or directory path (a stream path is rejected). |
| 3 | `SnapshotPin` | `varuint` | List inside the snapshot held by this reader pin; 0 = live view. |
| 4 | — | `extensions` | Terminal extension block. |

#### `ListStreamsResult` = 79 (`ListStreamsResultMessage`)

Reply to `ListStreamsMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Streams` | `list<StreamInfoData>` | Named streams, ordinal by name. |
| 2 | `ObservedAt` | `CommitToken` | Commit position the answer is based on. |
| 3 | — | `extensions` | Terminal extension block. |

### 9.5 Transactions

#### `TxBegin` = 80 (`TxBeginMessage`)

Begins a transaction (client → volume leader; committed `TxBegin` entry). The header operation id becomes the durable transaction id. The entry records the read index R, a pin and a staging session; every replica retains the state root at R while the transaction is active. Leader change, restart, expiry or limit violations abort the transaction (`DfsErrorCode.TransactionAborted`, retryable by the application).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` is the transaction's begin id (retries reuse it). |
| 2 | `Timeout` | `duration?` | Requested maximum duration (clamped to the volume limit); null = the volume limit. |
| 3 | — | `extensions` | Terminal extension block. |

#### `TxBeginResult` = 81 (`TxBeginResultMessage`)

Reply to `TxBeginMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Transaction` | `TransactionId` | Durable transaction id. |
| 2 | `ReadVersion` | `CommitToken` | Commit position R the transaction reads from. |
| 3 | `Staging` | `StagingId` | Staging session of the transaction (closed when it ends). |
| 4 | `Capability` | `bytes` | Encoded staging `DataCapability` for the transaction's uploads. |
| 5 | `CapabilityExpiry` | `timestamp` | Capability expiry (renew with `CreateStaging` and `Renew`). |
| 6 | `Deadline` | `timestamp` | Leader wall-clock time after which the transaction is aborted. |
| 7 | `PlacementEpoch` | `varuint` | Placement epoch for uploads. |
| 8 | `Limits` | `VolumeLimits` | Transaction limits in force. |
| 9 | — | `extensions` | Terminal extension block. |

#### `TxOp` = 82 (`TxOpMessage`)

Optional batching envelope: executes several transaction-bound requests in order against the transaction's working root (client → volume leader). Each element is a complete request frame whose header carries the same transaction; execution stops at the first failing request, whose error reply is the last element of the result. Elements are decoded one level deep only: an element that is itself a `TxOp`, `TxBegin`, `TxCommit` or `TxAbort`, or whose header names another transaction or volume, is answered with `DfsErrorCode.InvalidArgument` (no nesting, so a hostile frame cannot force recursive decoding). Ordinary transaction-bound requests do not need this envelope; a server that does not implement it answers `DfsErrorCode.Unsupported`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Transaction` identifies the transaction. |
| 2 | `Requests` | `list<bytes>` | Encoded request frames. |
| 3 | — | `extensions` | Terminal extension block. |

#### `TxOpResult` = 83 (`TxOpResultMessage`)

Reply to `TxOpMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Replies` | `list<bytes>` | Encoded reply frames, one per executed request (the last one may be an error reply). |
| 2 | — | `extensions` | Terminal extension block. |

#### `TxCommit` = 84 (`TxCommitMessage`)

Commits a transaction (client → volume leader; committed `TxCommit` entry with the read set and ordered writes). Apply validates every read item against the current state; any change → `DfsErrorCode.TransactionConflict` with no effect, otherwise all writes apply atomically at the commit index (strict serializability). Retries reuse the header operation id; an unknown outcome is resolved with `QueryOperation(IsTransaction: true)`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Transaction` is the transaction and `RequestHeader.Operation` the commit attempt id. |
| 2 | `SnapshotName` | `string?` | Commit-and-snapshot: create a snapshot with this name in the same decision; null for none. |
| 3 | — | `extensions` | Terminal extension block. |

#### `TxCommitResult` = 85 (`TxCommitResultMessage`)

Reply to `TxCommitMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Token` | `CommitToken` | Commit token of the transaction. |
| 2 | `Snapshot` | `SnapshotInfoData?` | Snapshot created by commit-and-snapshot, or null. |
| 3 | — | `extensions` | Terminal extension block. |

#### `TxAbort` = 86 (`TxAbortMessage`)

Aborts a transaction (client → volume leader; committed `TxAbort` entry): releases reservations, pins and the staging session. Never reverses a committed decision: aborting a committed transaction reports its committed state. Idempotent.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Transaction` identifies the transaction. |
| 2 | — | `extensions` | Terminal extension block. |

#### `TxAbortResult` = 87 (`TxAbortResultMessage`)

Reply to `TxAbortMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `FinalStatus` | `enum WireOutcomeStatus` | Final state: Aborted, or Committed when the transaction had already committed. |
| 2 | `Token` | `CommitToken` | Commit token when committed; otherwise the abort entry's commit (or `CommitToken.None`). |
| 3 | — | `extensions` | Terminal extension block. |

### 9.6 Snapshots

#### `CreateSnapshot` = 90 (`CreateSnapshotMessage`)

Creates a named, immutable snapshot of the whole volume (client → volume leader; committed `CreateSnapshot` entry; O(1) capture of the current root). With `MinimumCommit` the leader waits until it has applied at least that token (same cluster/volume/epoch, else EpochMismatch) before proposing. Adapters run client-side; the label is `DfsConsistencyLabel.ApplicationConsistent` only if every declared adapter prepared successfully. Errors: AlreadyExists (name), EpochMismatch, AccessDenied (needs Snapshot permission), ReadOnly (backup host).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the creation. |
| 2 | `Name` | `string` | Snapshot name (unique per volume). |
| 3 | `MinimumCommit` | `CommitToken?` | Boundary must be at or after this commit; null = current state. |
| 4 | `Label` | `enum DfsConsistencyLabel` | Requested consistency label. |
| 5 | `Adapters` | `list<AdapterRecordData>` | Adapters that prepared successfully (recorded with the snapshot). |
| 6 | `Retention` | `Retention?` | Initial retention; null = none. |
| 7 | — | `extensions` | Terminal extension block. |

#### `CreateSnapshotResult` = 91 (`CreateSnapshotResultMessage`)

Reply to `CreateSnapshotMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Snapshot` | `SnapshotInfoData` | The created snapshot (its boundary is the creating entry). |
| 2 | — | `extensions` | Terminal extension block. |

#### `ListSnapshots` = 92 (`ListSnapshotsMessage`)

Lists snapshots of a volume (client → volume leader or backup node; ReadIndex read). On a backup node the list contains the published snapshots it retains.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `Name` | `string?` | Only the snapshot with this name; null = all. |
| 3 | — | `extensions` | Terminal extension block. |

#### `ListSnapshotsResult` = 93 (`ListSnapshotsResultMessage`)

Reply to `ListSnapshotsMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Snapshots` | `list<SnapshotInfoData>` | Snapshots in ascending boundary order. |
| 2 | `ObservedAt` | `CommitToken` | Commit position the list is based on (`CommitToken.None` on backup nodes). |
| 3 | — | `extensions` | Terminal extension block. |

#### `DeleteSnapshot` = 94 (`DeleteSnapshotMessage`)

Deletes a snapshot's retention reference (client → volume leader or backup node; committed `DeleteSnapshot` entry). Data still needed by other snapshots, the current state, readers or transfers is kept; GC decides reclamation. Errors: SnapshotNotFound, RetentionProtected (expiry in the future or hold), AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the deletion. |
| 2 | `Name` | `string` | Snapshot name. |
| 3 | — | `extensions` | Terminal extension block. |

#### `DeleteSnapshotResult` = 95 (`DeleteSnapshotResultMessage`)

Reply to `DeleteSnapshotMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Existed` | `bool` | True when the snapshot existed and was deleted. |
| 2 | `Token` | `CommitToken` | Commit of the deletion. |
| 3 | — | `extensions` | Terminal extension block. |

#### `OpenSnapshot` = 96 (`OpenSnapshotMessage`)

Opens a read-only snapshot view (client → volume leader or backup node; committed `AcquirePin` entry on live volumes, a local pin on backup nodes). Returns a reader pin owned by the caller's session and renewed by `KeepAlive`; reads address the snapshot with it. Errors: SnapshotNotFound, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (`RequestHeader.Session` owns the pin). |
| 2 | `Name` | `string` | Snapshot name. |
| 3 | — | `extensions` | Terminal extension block. |

#### `OpenSnapshotResult` = 97 (`OpenSnapshotResultMessage`)

Reply to `OpenSnapshotMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `SnapshotPin` | `varuint` | Reader pin id (non-zero). |
| 2 | `Snapshot` | `SnapshotInfoData` | The opened snapshot. |
| 3 | `PinDuration` | `duration` | Pin lifetime without renewal. |
| 4 | — | `extensions` | Terminal extension block. |

#### `CloseSnapshot` = 98 (`CloseSnapshotMessage`)

Releases a reader pin owned by the caller's session (client → volume leader or backup node): a snapshot reader pin from `OpenSnapshot`, or an enumeration pin from `ListDirectory` when an enumeration is abandoned early. Idempotent.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header. |
| 2 | `SnapshotPin` | `varuint` | Reader pin to release. |
| 3 | — | `extensions` | Terminal extension block. |

#### `CloseSnapshotResult` = 99 (`CloseSnapshotResultMessage`)

Reply to `CloseSnapshotMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Released` | `bool` | True when the pin was held and is now released. |
| 2 | — | `extensions` | Terminal extension block. |

#### `SetRetention` = 100 (`SetRetentionMessage`)

Changes a snapshot's retention (client → volume leader or backup node; committed `SetSnapshotRetention` entry). Shortening an expiry or removing a hold requires RetentionAdmin; on backup nodes export credentials can never shorten protected retention (BAK-022). Errors: SnapshotNotFound, RetentionProtected, AccessDenied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the change. |
| 2 | `Name` | `string` | Snapshot name. |
| 3 | `Retention` | `Retention` | New retention. |
| 4 | — | `extensions` | Terminal extension block. |

#### `SetRetentionResult` = 101 (`SetRetentionResultMessage`)

Reply to `SetRetentionMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Snapshot` | `SnapshotInfoData` | The snapshot with its new retention. |
| 2 | `Token` | `CommitToken` | Commit of the change. |
| 3 | — | `extensions` | Terminal extension block. |

#### `RestoreSnapshot` = 102 (`RestoreSnapshotMessage`)

Restores a snapshot into a new volume (client → volume leader, which coordinates with the control leader, §8.5): new VolumeId, epoch 1, initial state = the snapshot root, objects copied server-side to new ids owned by the new volume (`CopyObjects`, or read and re-placed when the new volume lives in another storage pool). Never rolls a live volume back in place. The new volume is not served before the restore is complete. Errors: SnapshotNotFound, AlreadyExists (volume name), PoolNotFound, InvalidPolicy (the pool cannot satisfy the restored policies), PlacementUnavailable, LimitExceeded (`MaxConcurrentRestores`), AccessDenied (needs VolumeAdmin and ClusterAdmin).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header; `RequestHeader.Operation` identifies the restore. |
| 2 | `Name` | `string` | Snapshot name. |
| 3 | `NewVolumeName` | `string` | Name of the new volume. |
| 4 | — | `extensions` | Terminal extension block (field 1, see below). |

Extension fields (added after v1 shipped, §2.5):

| Field id | Flags | Value | Meaning |
|---|---|---|---|
| 1 `StoragePool` | Required | UTF-8 pool name | Bind the new volume to this storage pool (Addendum A, POOL-02/POOL-016): data objects are copied only onto the pool's nodes and the metadata voters are chosen from them; the restored policies must be satisfiable by the pool (`InvalidPolicy`), an unknown pool is `PoolNotFound`. Absent: the new volume keeps the source volume's pool. Required so that a receiver that does not implement it refuses the request (`Unsupported`) instead of restoring into the wrong pool. Sent by the harness's `TestCluster.RestoreSnapshotToPoolAsync`. |

#### `RestoreSnapshotResult` = 103 (`RestoreSnapshotResultMessage`)

Reply to `RestoreSnapshotMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeEntry` | The new volume. |
| 2 | `LastCommit` | `CommitToken` | Commit of the new volume's initial state. |
| 3 | — | `extensions` | Terminal extension block. |

#### `RestoreVolume` = 104 (`RestoreVolumeMessage`)

Control-plane step of a restore (restore coordinator → control leader; node peers only, §8.5). The coordinator authorized the requesting principal (VolumeAdmin of the source and ClusterAdmin) before sending; the control leader re-checks that the principal is a cluster administrator. `Action`: `Create` (0) creates the new volume in the `Restoring` state with the source volume's metadata policy, current default and named storage policies, inline threshold, limits and traverse setting, bound to `StoragePool` (voters chosen from that pool); `Activate` (1) makes a restoring volume `Active` (F21 `Reconfiguration` stage, operation `RestoreSnapshot`); `Abandon` (2) removes a volume that is still `Restoring`. Every action is deduplicated by `RequestId` (a retried create returns the volume created by the first attempt; a failed one returns its failure). Errors: AlreadyExists, InvalidArgument (volume name, action), PoolNotFound, InvalidPolicy, PlacementUnavailable (no voters), VolumeNotFound, AccessDenied, NotLeader.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `RequestId` | `u128` | Deduplication id of the action (derived by the coordinator from the source volume, the restore's operation id and the action). |
| 2 | `Action` | `enum WireRestoreAction` | Step to perform: `Create` = 0, `Activate` = 1, `Abandon` = 2. |
| 3 | `SourceVolume` | `VolumeId` | Volume whose snapshot is restored. |
| 4 | `Volume` | `VolumeId` | The restoring volume (`Activate`, `Abandon`); empty for `Create`. |
| 5 | `NewVolumeName` | `string` | Name of the new volume. |
| 6 | `StoragePool` | `string?` | Storage pool of the new volume; null = the source volume's pool. |
| 7 | `Principal` | `string` | Principal that requested the restore (becomes the creator of the new volume and receives every grant on it). |
| 8 | — | `extensions` | Terminal extension block. |

#### `RestoreVolumeResult` = 105 (`RestoreVolumeResultMessage`)

Reply to `RestoreVolumeMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeEntry?` | The restoring (after `Create`) or activated (after `Activate`) volume; null after `Abandon`. |
| 2 | — | `extensions` | Terminal extension block. |

#### `RestoreInstall` = 106 (`RestoreInstallMessage`)

Installs a restored namespace into a `Restoring` volume (restore coordinator → the new volume's leader; node peers only, §8.5). `Phase`: `Begin` (0) opens the restore bound to `Source` (idempotent) and returns the staging session the copies are stored under (it stays open, protecting the copies from GC, until the install completes); `Stage` (1) stages a chunk of inode records (committed volume entries tagged `RestoreSnapshot`, invisible until complete); `Complete` (2) installs the staged inodes atomically in one entry after checking that they form one tree under the root and that `InodeCount` were staged. Every phase is idempotent for the same `Source`; a `Begin` after completion reports the installed state. Errors: VolumeNotFound, InvalidArgument (not a restoring volume, another source, malformed records), Integrity (staged count mismatch or inconsistent tree), NotLeader.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeId` | The restoring volume. |
| 2 | `Source` | `string` | Opaque restore source tag binding all phases of one restore. |
| 3 | `Phase` | `enum WireRestorePhase` | Install step: `Begin` = 0, `Stage` = 1, `Complete` = 2. |
| 4 | `Staging` | `StagingId` | The restore staging session (`Stage`, `Complete`); empty for `Begin`. |
| 5 | `Inodes` | `bytes` | `Stage`: a format byte (1) followed by `InodeCount` inode records in the server's volume-state codec; empty otherwise. |
| 6 | `InodeCount` | `varint32` | `Stage`: records in `Inodes` (1..4096); `Complete`: total number of inodes the namespace must contain. |
| 7 | — | `extensions` | Terminal extension block. |

#### `RestoreInstallResult` = 107 (`RestoreInstallResultMessage`)

Reply to `RestoreInstallMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Staging` | `StagingId` | The restore staging session. |
| 2 | `StagedCount` | `varint32` | Inodes staged so far (after completion: inodes installed). |
| 3 | `Token` | `CommitToken` | Commit of the installed namespace once the restore is complete; `CommitToken.None` while it is not. |
| 4 | — | `extensions` | Terminal extension block. |

### 9.7 Data plane

#### `PutObject` = 110 (`PutObjectMessage`)

Stores one immutable object (client or node → storage node). The node verifies the staging capability (MAC, cluster, volume, epoch, staging session, principal, expiry), the declared length and checksum against the payload, and capacity admission (CapacityExhausted before touching reserves), writes the record and flushes, and only then returns a signed receipt. A receipt is always bound to the request's staging session. Idempotent: re-uploading identical content under the same staging session returns a receipt; under another staging session the node first durably re-binds its copy to the new session (a new record), so a garbage decision taken for the old (object, staging) pair can no longer remove it (see `ObjectStagingRef`). The same id with different content is rejected with `DfsErrorCode.Integrity`. Objects are at most `MaxObjectSize` bytes.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Capability` | `bytes` | Encoded staging `DataCapability` (may be empty only for authenticated node peers). |
| 2 | `Volume` | `VolumeId` | Owning volume. |
| 3 | `Staging` | `StagingId` | Staging session (must match the capability). |
| 4 | `Object` | `ObjectId` | Object id chosen by the uploader (random, non-zero). |
| 5 | `Checksum` | `u128` | XxHash128 of `Data`. |
| 6 | `Length` | `varint` | Length of `Data`. |
| 7 | `Encoding` | `u8` | Object encoding (`ObjectEncodings`). |
| 8 | `PlacementEpoch` | `varuint` | Placement epoch the uploader computed targets under. |
| 9 | `Data` | `bytes` | Object payload (plaintext; nodes encrypt at rest). |
| 10 | — | `extensions` | Terminal extension block. |

#### `PutObjectResult` = 111 (`PutObjectResultMessage`)

Reply to `PutObjectMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Receipt` | `ObjectReceipt` | Signed durability receipt. |
| 2 | `AlreadyPresent` | `bool` | True when an identical copy was already stored. |
| 3 | — | `extensions` | Terminal extension block. |

#### `GetObject` = 112 (`GetObjectMessage`)

Reads a byte range of an object (client or node → storage node). Requires a read capability listing the object (nodes may omit it); object ids alone grant nothing (SEC-004). The node verifies the stored record before replying (corruption → `DfsErrorCode.Integrity`, the client tries another replica); a missing object is `DfsErrorCode.Integrity` with detail "missing", never zeroes.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Capability` | `bytes` | Encoded read `DataCapability`. |
| 2 | `Volume` | `VolumeId` | Owning volume. |
| 3 | `Object` | `ObjectId` | Object to read. |
| 4 | `Offset` | `varint` | First byte to read. |
| 5 | `Length` | `varint` | Bytes to read (clipped to the object end). |
| 6 | — | `extensions` | Terminal extension block. |

#### `GetObjectResult` = 113 (`GetObjectResultMessage`)

Reply to `GetObjectMessage`. When `UnitSize` is positive, `Offset` is aligned down to a unit boundary, `Data` covers the requested range extended to unit boundaries (clipped to the object end), and `UnitChecksums[i]` is the CRC32C of unit i of `Data`, letting the client verify a partial range; a whole-object read is verified against `Checksum`. Only the last unit of `Data` may be shorter than `UnitSize` (the object end, or a verified short result that covers a prefix of the requested range, FSD §6.2); its checksum covers exactly the bytes present, and the client continues from the first byte not returned. When `UnitSize` is 0, `Offset` is the requested offset and no unit checksums are supplied.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ObjectLength` | `varint` | Full plaintext length of the object. |
| 2 | `Checksum` | `u128` | XxHash128 of the whole object plaintext. |
| 3 | `Encoding` | `u8` | Object encoding. |
| 4 | `Offset` | `varint` | Object offset of the first byte of `Data`. |
| 5 | `Data` | `bytes` | Object bytes. |
| 6 | `UnitSize` | `varint32` | Verification unit size (0 = none). |
| 7 | `UnitChecksums` | `list<u32>` | CRC32C of each unit of `Data`. |
| 8 | — | `extensions` | Terminal extension block. |

#### `LocateObject` = 114 (`LocateObjectMessage`)

Asks a storage node which of the listed objects it holds (client or node → storage node; read fallback broadcast and repair). Requires Read permission on the volume.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeId` | Owning volume. |
| 2 | `Objects` | `list<ObjectId>` | Objects to look up. |
| 3 | — | `extensions` | Terminal extension block. |

#### `LocateObjectResult` = 115 (`LocateObjectResultMessage`)

Reply to `LocateObjectMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Locations` | `list<ObjectLocation>` | One answer per requested object, in request order. |
| 2 | — | `extensions` | Terminal extension block. |

#### `ReplicateObject` = 116 (`ReplicateObjectMessage`)

Repair pull (volume leader → target storage node): fetch the object from one of the sources (node-authenticated `GetObject`), verify length and checksum, store it durably and return a receipt. Idempotent.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeId` | Owning volume. |
| 2 | `Object` | `ObjectId` | Object to replicate. |
| 3 | `Staging` | `StagingId` | Staging session the object was originally stored under (kept for GC decisions). |
| 4 | `Checksum` | `u128` | Expected XxHash128 of the plaintext. |
| 5 | `Length` | `varint` | Expected plaintext length. |
| 6 | `Encoding` | `u8` | Object encoding. |
| 7 | `PlacementEpoch` | `varuint` | Placement epoch of the repair decision. |
| 8 | `Sources` | `list<NodeId>` | Nodes believed to hold verified copies, preferred first. |
| 9 | — | `extensions` | Terminal extension block. |

#### `ReplicateObjectResult` = 117 (`ReplicateObjectResultMessage`)

Reply to `ReplicateObjectMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Receipt` | `ObjectReceipt` | Durability receipt of the new copy. |
| 2 | `AlreadyPresent` | `bool` | True when the node already held a verified copy. |
| 3 | — | `extensions` | Terminal extension block. |

#### `DeleteObjects` = 118 (`DeleteObjectsMessage`)

Deletes stored copies (volume leader → storage node): garbage the leader proved unreachable at a committed index with a closed staging session (GC), or excess replicas after repair verified the targets (REP-016). Copies are identified by (object, staging session), the unit of GC decisions (ARCHITECTURE §8): a node deletes a copy only while its stored record still carries that staging session, and keeps a copy that a later upload re-bound to another session. Rejected for an older volume epoch. Idempotent.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeId` | Owning volume. |
| 2 | `VolumeEpoch` | `varuint` | Volume epoch of the decision. |
| 3 | `DecisionIndex` | `varuint` | Applied index at which deletability was decided. |
| 4 | `Objects` | `list<ObjectStagingRef>` | Copies to delete. |
| 5 | `ExcessReplicas` | `bool` | True for excess-replica cleanup rather than garbage collection. |
| 6 | — | `extensions` | Terminal extension block. |

#### `DeleteObjectsResult` = 119 (`DeleteObjectsResultMessage`)

Reply to `DeleteObjectsMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Deleted` | `varint32` | Objects deleted. |
| 2 | `Absent` | `varint32` | Objects that were not present. |
| 3 | — | `extensions` | Terminal extension block. |

#### `InventoryReport` = 120 (`InventoryReportMessage`)

Anti-entropy inventory (storage node → volume leader), paged. The leader updates its object-location map, schedules repair for deficits, and after the last page answers which reported objects are deletable garbage.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Node` | `NodeId` | Reporting node. |
| 2 | `Incarnation` | `varuint` | Reporting node incarnation (stale incarnations are rejected). |
| 3 | `Volume` | `VolumeId` | Volume whose objects are reported. |
| 4 | `ReportId` | `varuint` | Identifier of this full report (all pages share it). |
| 5 | `Page` | `varint32` | Page index, starting at 0. |
| 6 | `LastPage` | `bool` | True on the final page. |
| 7 | `Items` | `list<InventoryItem>` | Objects on this page. |
| 8 | — | `extensions` | Terminal extension block. |

#### `InventoryReportResult` = 121 (`InventoryReportResultMessage`)

Reply to `InventoryReportMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Delete` | `list<ObjectStagingRef>` | Reported copies (object, staging session) that are garbage at `DecisionIndex` (only on the last page); deleted with the same rule as `DeleteObjectsMessage`. |
| 2 | `Unknown` | `list<ObjectId>` | Reported objects the leader cannot classify yet (for example a staging id newer than its applied state); keep them. |
| 3 | `DecisionIndex` | `varuint` | Applied index of the GC decision (0 when `Delete` is empty). |
| 4 | `VolumeEpoch` | `varuint` | Volume epoch of the decision. As for `DeleteObjectsMessage`, a node ignores the `Delete` list when the epoch is older than the volume epoch it knows (a decision of a superseded authority). |
| 5 | — | `extensions` | Terminal extension block. |

#### `CopyObjects` = 122 (`CopyObjectsMessage`)

Server-side object copy for restore (restore coordinator → storage node holding the sources): each source object is copied locally to a new id owned by the target volume and stored durably. Idempotent per (from, to).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `SourceVolume` | `VolumeId` | Volume owning the source objects. |
| 2 | `TargetVolume` | `VolumeId` | Volume that will own the copies. |
| 3 | `TargetStaging` | `StagingId` | Staging session of the target volume the copies are stored under. |
| 4 | `PlacementEpoch` | `varuint` | Placement epoch of the copies. |
| 5 | `Copies` | `list<ObjectCopy>` | Copies to make. |
| 6 | — | `extensions` | Terminal extension block. |

#### `CopyObjectsResult` = 123 (`CopyObjectsResultMessage`)

Reply to `CopyObjectsMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Receipts` | `list<ObjectReceipt>` | Receipts of the stored copies. |
| 2 | `MissingSources` | `list<ObjectId>` | Source objects this node does not hold. |
| 3 | — | `extensions` | Terminal extension block. |

### 9.8 Consensus

#### `RaftBatch` = 130 (`RaftBatchMessage`)

Raft messages from one node to another, batched per destination (node → node). Normally sent one-way; when sent as a request it is answered by `RaftBatchResultMessage`. Receivers drop messages from unknown or removed members, from an incarnation older than the registered one, from a process whose incarnation equals the registered one but whose boot id differs (a cloned disk, RSY-021), and for groups the receiver does not host. A newer, not yet registered incarnation is accepted: a restarted voter must take part in the elections and commits that register it (after a full-cluster restart every voter is in that state). The sender must persist hard state and entries before sending messages that depend on them (CON-004).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `From` | `NodeId` | Sending node (must equal the authenticated connection identity). |
| 2 | `Incarnation` | `varuint` | Sending node incarnation. |
| 3 | `Messages` | `list<RaftEnvelope>` | Messages in send order. |
| 4 | — | `extensions` | Terminal extension block. |

#### `RaftBatchResult` = 131 (`RaftBatchResultMessage`)

Reply to a `RaftBatchMessage` sent as a request.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Receiver` | `NodeId` | Receiving node. |
| 2 | `ReceiverIncarnation` | `varuint` | Receiving node incarnation. |
| 3 | `Accepted` | `varint32` | Number of messages delivered to a hosted group. |
| 4 | `UnknownGroups` | `list<string>` | Groups named in the batch that the receiver does not host. |
| 5 | — | `extensions` | Terminal extension block. |

#### `RaftSnapshotChunk` = 132 (`RaftSnapshotChunkMessage`)

One chunk of a Raft snapshot transfer as a request/response pair (leader → lagging follower), giving the sender flow control for large checkpoints; equivalent to an InstallSnapshot message plus its response. Chunks are sent in offset order; the follower installs the snapshot atomically after the final chunk and reports the next expected offset (equal to the snapshot length once installed).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `From` | `NodeId` | Leader node. |
| 2 | `Incarnation` | `varuint` | Leader node incarnation. |
| 3 | `Group` | `string` | Group name. |
| 4 | `To` | `NodeId` | Follower node. |
| 5 | `Term` | `varuint` | Leader term. |
| 6 | `Meta` | `RaftWireSnapshotMeta` | Snapshot metadata. |
| 7 | `Offset` | `varint` | Byte offset of `Data` in the snapshot payload. |
| 8 | `Data` | `bytes` | Chunk bytes. |
| 9 | `Done` | `bool` | True for the final chunk. |
| 10 | — | `extensions` | Terminal extension block. |

#### `RaftSnapshotChunkResult` = 133 (`RaftSnapshotChunkResultMessage`)

Reply to `RaftSnapshotChunkMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Term` | `varuint` | Follower term. |
| 2 | `SnapshotIndex` | `varuint` | Index of the snapshot being transferred. |
| 3 | `NextOffset` | `varint` | Next byte offset the follower expects. |
| 4 | `Reject` | `bool` | True when the chunk was refused (stale snapshot, out-of-order offset, install failure). |
| 5 | — | `extensions` | Terminal extension block. |

### 9.9 Administration

#### `AdminRequest` = 140 (`AdminRequestMessage`)

Administrative request (admin client → any live node; control-group operations are forwarded to or must be sent to the control leader, which answers NotLeader with a hint otherwise). The body is the encoded body record of `Op` (see `AdminOp`). Mutating operations are deduplicated by `RequestId` while the control leader retains it. Requires ClusterAdmin (volume operations accept VolumeAdmin on that volume). Unknown operations are answered with `DfsErrorCode.Unsupported`; failures use the error reply.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Op` | `enum AdminOp (raw)` | Operation. |
| 2 | `RequestId` | `u128` | Random id chosen once per logical request and reused by retries. |
| 3 | `Body` | `bytes` | Encoded request body record. |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminResult` = 141 (`AdminResultMessage`)

Successful reply to `AdminRequestMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Op` | `enum AdminOp (raw)` | Operation answered. |
| 2 | `Body` | `bytes` | Encoded result body record. |
| 3 | — | `extensions` | Terminal extension block. |

### 9.10 Backup

#### `BackupRegister` = 150 (`BackupRegisterMessage`)

Manages backup target registrations of a volume (client → volume leader; committed entries for mutating actions). Registration stores endpoints, schedule, budgets and the export credentials; removal releases transfer pins. Requires BackupAdmin. Backup targets never join any Raft group (BAK-001).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (volume scoping; `RequestHeader.Operation` identifies mutations). |
| 2 | `Action` | `enum BackupTargetAction` | What to do. |
| 3 | `Target` | `string` | Target name (ignored for List). |
| 4 | `Endpoints` | `list<string>` | Target endpoints (Register). |
| 5 | `ExportCredentials` | `Credentials?` | Credentials the exporter authenticates with on the target (Register). |
| 6 | `Schedule` | `BackupScheduleData?` | Schedule (Register, Configure). |
| 7 | `Budgets` | `BackupBudgetsData?` | Budgets (Register, Configure). |
| 8 | — | `extensions` | Terminal extension block. |

#### `BackupRegisterResult` = 151 (`BackupRegisterResultMessage`)

Reply to `BackupRegisterMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Targets` | `list<string>` | Registered target names after the action. |
| 2 | `Token` | `CommitToken` | Commit of the change (`CommitToken.None` for List). |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupStatus` = 152 (`BackupStatusMessage`)

Reads the status of one backup target (client → volume leader). Requires BackupAdmin or Read.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (volume scoping; `RequestHeader.Operation` identifies mutations). |
| 2 | `Target` | `string` | Target name. |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupStatusResult` = 153 (`BackupStatusResultMessage`)

Reply to `BackupStatusMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Status` | `BackupStatusData` | Target status (received, durable and published boundaries reported separately, BAK-016). |
| 2 | — | `extensions` | Terminal extension block. |

#### `BackupRequestSync` = 154 (`BackupRequestSyncMessage`)

Asks the exporter to synchronize a target now (client → volume leader).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (volume scoping; `RequestHeader.Operation` identifies mutations). |
| 2 | `Target` | `string` | Target name. |
| 3 | `SnapshotName` | `string?` | Snapshot to export; null = create/pick the export snapshot. |
| 4 | — | `extensions` | Terminal extension block. |

#### `BackupRequestSyncResult` = 155 (`BackupRequestSyncResultMessage`)

Reply to `BackupRequestSyncMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Accepted` | `bool` | True when a transfer was scheduled. |
| 2 | `Token` | `CommitToken` | Boundary of the snapshot scheduled for transfer. |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupAwait` = 156 (`BackupAwaitMessage`)

Waits for a backup recovery boundary (client → volume leader). With `Commit`: the first published checkpoint of the same volume epoch covering at least that commit; with `SnapshotName`: that exact snapshot durably retained. The leader waits at most `Timeout` and then answers `Reached = false`; a timeout never rolls back the live commit (BAK-019).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Header` | `RequestHeader` | Request header (volume scoping; `RequestHeader.Operation` identifies mutations). |
| 2 | `Target` | `string` | Target name. |
| 3 | `Commit` | `CommitToken?` | Commit boundary to await (exclusive with `SnapshotName`). |
| 4 | `SnapshotName` | `string?` | Exact snapshot to await (exclusive with `Commit`). |
| 5 | `Timeout` | `duration` | Maximum time the leader waits before answering. |
| 6 | — | `extensions` | Terminal extension block. |

#### `BackupAwaitResult` = 157 (`BackupAwaitResultMessage`)

Reply to `BackupAwaitMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Reached` | `bool` | True when the requested boundary is published. |
| 2 | `Checkpoint` | `BackupCheckpoint?` | The satisfying checkpoint (actual snapshot, covered commit, retention). |
| 3 | `LastPublished` | `BackupCheckpoint?` | Latest published checkpoint (reported when not reached). |
| 4 | — | `extensions` | Terminal extension block. |

#### `BackupTargetUpdate` = 158 (`BackupTargetUpdateMessage`)

Applies a backup target registration change of a volume to the control state (volume leader serving `BackupRegister` → control leader; node peers only, §8.6). The volume leader authorized the requesting principal (BackupAdmin on the volume) and wrapped the export secret with the cluster key-encryption key before sending; the control leader re-checks the grant on its own state. `Register` and `Configure` store the complete registration (`Configure` requires an existing target); `Remove` removes it; `List` is not allowed. Deduplicated by `RequestId` through the control group's administrative outcomes. Errors: VolumeNotFound, BackupTargetNotFound, InvalidArgument, AccessDenied, NotLeader.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `RequestId` | `u128` | Deduplication id (derived by the volume leader from the volume and the request's operation id). |
| 2 | `Volume` | `VolumeId` | The volume. |
| 3 | `Action` | `enum BackupTargetAction` | Change to apply (`List` is not allowed). |
| 4 | `Principal` | `string` | Principal that requested the change. |
| 5 | `Target` | `string` | Target name. |
| 6 | `Endpoints` | `list<string>` | Target endpoints (Register, Configure). |
| 7 | `ExportPrincipal` | `string` | Principal the exporter authenticates as on the target (Register, Configure). |
| 8 | `WrappedExportSecret` | `bytes` | The export principal's secret wrapped with the cluster key-encryption key (Register, Configure). |
| 9 | `Schedule` | `BackupScheduleData?` | Schedule (Register, Configure). |
| 10 | `Budgets` | `BackupBudgetsData?` | Budgets (Register, Configure). |
| 11 | — | `extensions` | Terminal extension block. |

#### `BackupTargetUpdateResult` = 159 (`BackupTargetUpdateResultMessage`)

Reply to `BackupTargetUpdateMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Targets` | `list<string>` | Registered target names of the volume after the change. |
| 2 | `ControlIndex` | `varuint` | Control-log index of the change (the volume leader waits until its own control replica applied it). |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupBegin` = 160 (`BackupBeginMessage`)

Starts (or resumes) the transfer of one snapshot (exporter on the volume leader → backup node, authenticated with the export credentials). The target answers whether it already published the snapshot and whether the lineage must be reseeded. Transfers are resumable and idempotent per transfer key (BAK-009).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Key` | `BackupTransferKey` | Transfer identity. |
| 2 | `SourceClusterName` | `string` | Source cluster name. |
| 3 | `VolumeName` | `string` | Source volume name. |
| 4 | `SnapshotName` | `string` | Snapshot name. |
| 5 | `Boundary` | `CommitToken` | Commit boundary of the snapshot. |
| 6 | `CreatedUtc` | `timestamp` | Snapshot creation time. |
| 7 | `Label` | `enum DfsConsistencyLabel` | Consistency label. |
| 8 | `Adapters` | `list<AdapterRecordData>` | Adapter records of the snapshot. |
| 9 | `Retention` | `Retention` | Retention requested by the source (the target applies its own policy on top). |
| 10 | `BaseSnapshot` | `SnapshotId?` | Previously published snapshot of the same lineage this transfer is incremental to; null = full. |
| 11 | `FullReseed` | `bool` | True when the source starts a new lineage after abandoning the old one. |
| 12 | — | `extensions` | Terminal extension block. |

#### `BackupBeginResult` = 161 (`BackupBeginResultMessage`)

Reply to `BackupBeginMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Accepted` | `bool` | True when the target accepts the transfer. |
| 2 | `AlreadyPublished` | `bool` | True when the snapshot is already published (nothing to send). |
| 3 | `ReseedRequired` | `bool` | True when the target cannot continue the lineage (send a full reseed). |
| 4 | `CurrentLineage` | `guid` | Lineage the target currently follows. |
| 5 | `LastPublished` | `SnapshotId?` | Latest snapshot the target published for the volume. |
| 6 | `KnownObjects` | `varint` | Objects the target already holds for the volume (hint only). |
| 7 | `ReceivedBytes` | `varint` | Bytes of this transfer already received (resume point). |
| 8 | — | `extensions` | Terminal extension block. |

#### `BackupInventory` = 162 (`BackupInventoryMessage`)

Asks the target which of the listed objects it still needs (exporter → backup node). The answer is only a hint: the target verifies the complete dependency closure itself before publishing (BAK-007).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Key` | `BackupTransferKey` | Transfer identity. |
| 2 | `Objects` | `list<ObjectId>` | Objects referenced by the snapshot. |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupInventoryResult` = 163 (`BackupInventoryResultMessage`)

Reply to `BackupInventoryMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Missing` | `list<ObjectId>` | Listed objects the target does not hold verified. |
| 2 | — | `extensions` | Terminal extension block. |

#### `BackupPutObject` = 164 (`BackupPutObjectMessage`)

Transfers one object (exporter → backup node). The target verifies length and checksum, persists it durably and only then reports it durable. Idempotent.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Key` | `BackupTransferKey` | Transfer identity. |
| 2 | `Object` | `ObjectId` | Object id. |
| 3 | `Checksum` | `u128` | XxHash128 of the plaintext. |
| 4 | `Length` | `varint` | Plaintext length. |
| 5 | `Encoding` | `u8` | Object encoding. |
| 6 | `Data` | `bytes` | Object plaintext (the target encrypts at rest with its own keys). |
| 7 | — | `extensions` | Terminal extension block. |

#### `BackupPutObjectResult` = 165 (`BackupPutObjectResultMessage`)

Reply to `BackupPutObjectMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Durable` | `bool` | True when the object is durably stored. |
| 2 | `AlreadyPresent` | `bool` | True when a verified copy already existed. |
| 3 | `ReceivedBytes` | `varint` | Bytes received in this transfer so far. |
| 4 | — | `extensions` | Terminal extension block. |

#### `BackupPutManifest` = 166 (`BackupPutManifestMessage`)

Transfers a chunk of the snapshot manifest (exporter → backup node), in offset order. The manifest (namespace, extent maps, policies, adapter metadata, format version) is opaque to the protocol; its format is owned by the backup module and is checksummed as a whole.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Key` | `BackupTransferKey` | Transfer identity. |
| 2 | `Offset` | `varint` | Offset of `Data` in the manifest. |
| 3 | `TotalLength` | `varint` | Total manifest length. |
| 4 | `ManifestChecksum` | `u128` | XxHash128 of the whole manifest. |
| 5 | `Data` | `bytes` | Chunk bytes. |
| 6 | `Last` | `bool` | True for the final chunk. |
| 7 | — | `extensions` | Terminal extension block. |

#### `BackupPutManifestResult` = 167 (`BackupPutManifestResultMessage`)

Reply to `BackupPutManifestMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `NextOffset` | `varint` | Next manifest offset the target expects. |
| 2 | `Complete` | `bool` | True when the whole manifest is durable and its checksum verified. |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupPublish` = 168 (`BackupPublishMessage`)

Asks the target to publish the snapshot (exporter → backup node). The target verifies the full dependency closure itself, persists manifest and objects durably, atomically replaces its published pointer, and only then acknowledges. If anything is missing it does not publish and lists what is missing.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Key` | `BackupTransferKey` | Transfer identity. |
| 2 | `SnapshotName` | `string` | Snapshot name. |
| 3 | `Boundary` | `CommitToken` | Commit boundary covered. |
| 4 | `ManifestChecksum` | `u128` | Expected manifest checksum. |
| 5 | `ManifestLength` | `varint` | Expected manifest length. |
| 6 | `RequestedRetention` | `Retention` | Retention requested by the source. |
| 7 | — | `extensions` | Terminal extension block. |

#### `BackupPublishResult` = 169 (`BackupPublishResultMessage`)

Reply to `BackupPublishMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Published` | `bool` | True when the snapshot is published and durable. |
| 2 | `MissingObjects` | `list<ObjectId>` | Objects missing from the closure (when not published). |
| 3 | `ManifestIncomplete` | `bool` | True when the manifest is missing or fails verification. |
| 4 | `CoveredCommit` | `CommitToken` | Commit covered by the published snapshot. |
| 5 | `RetentionExpiryUtc` | `timestamp?` | Retention expiry enforced by the target. |
| 6 | `Held` | `bool` | True when the target holds the snapshot under a retention hold. |
| 7 | — | `extensions` | Terminal extension block. |

#### `BackupRetention` = 170 (`BackupRetentionMessage`)

Queries (and optionally extends) retention on a backup node (exporter or retention administrator → backup node). The backup node enforces retention with its own authorization: export credentials can extend but never shorten protected retention or delete protected snapshots (BAK-022).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `SourceClusterId` | `guid` | Source cluster. |
| 2 | `VolumeName` | `string` | Source volume name. |
| 3 | `SnapshotName` | `string?` | One snapshot; null = all retained snapshots of the volume. |
| 4 | `Extend` | `Retention?` | Retention to extend to (never shortens); null = query only. |
| 5 | — | `extensions` | Terminal extension block. |

#### `BackupRetentionResult` = 171 (`BackupRetentionResultMessage`)

Reply to `BackupRetentionMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Snapshots` | `list<BackupRetainedSnapshotData>` | Retained snapshots in ascending boundary order. |
| 2 | — | `extensions` | Terminal extension block. |

#### `BackupExportRestore` = 172 (`BackupExportRestoreMessage`)

Pulls a published snapshot from a backup node for restore (restoring cluster → backup node, authenticated as a reader principal). `Describe` pins the snapshot and returns its descriptor; `Manifest` and `Object` return chunks under that pin; `Release` drops the pin. Missing key material fails with `DfsErrorCode.RecoveryKeyMissing`, never with undecryptable bytes (RST-005).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `SourceVolume` | `string` | Source volume name. |
| 2 | `SnapshotName` | `string` | Published snapshot. |
| 3 | `Part` | `enum BackupRestorePart` | Requested part. |
| 4 | `ReaderPin` | `varuint` | Pin from `Describe` (0 for `Describe`). |
| 5 | `Object` | `ObjectId` | Object to read (`Object` part). |
| 6 | `Offset` | `varint` | Offset within the manifest or object. |
| 7 | `MaxBytes` | `varint32` | Maximum bytes to return. |
| 8 | `RecoveryKey` | `bytes` | Recovery key material when the backup node lacks its own (empty = none). |
| 9 | — | `extensions` | Terminal extension block. |

#### `BackupExportRestoreResult` = 173 (`BackupExportRestoreResultMessage`)

Reply to `BackupExportRestoreMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Part` | `enum BackupRestorePart` | Part answered. |
| 2 | `ReaderPin` | `varuint` | Reader pin (from `Describe`). |
| 3 | `Descriptor` | `BackupRestoreDescriptor?` | Snapshot descriptor (`Describe`). |
| 4 | `Offset` | `varint` | Offset of `Data`. |
| 5 | `TotalLength` | `varint` | Total length of the manifest or object. |
| 6 | `Checksum` | `u128` | XxHash128 of the whole manifest or object. |
| 7 | `Data` | `bytes` | Chunk bytes. |
| 8 | `Last` | `bool` | True when `Data` reaches the end. |
| 9 | — | `extensions` | Terminal extension block. |

## 10. Record reference

Nested records end with their own extension block. `MinEncodedSize` is the smallest possible encoding, used to bound collection counts before allocation.

#### `AdapterRecordData`

Identity and restore requirements of an application snapshot adapter recorded with a snapshot (FSD §11.3).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Id` | `string` | Adapter id. |
| 2 | `Version` | `string` | Adapter version. |
| 3 | `RestoreRequirements` | `map<string, string>` | Restore requirements declared by the adapter (ordinal keys). |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminAddNodeBody`

Request of `AdminOp.AddNode` (explicit admission; also re-admission of a removed identity under a new lineage).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Node name. |
| 2 | `Labels` | `Labels` | Failure-domain labels. |
| 3 | `Endpoints` | `list<string>` | Endpoints of the node. |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminAuditLogRequestBody`

Request of `AdminOp.AuditLog`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Max` | `varint32` | Maximum number of most recent entries. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminClusterStatusBody`

Result of `AdminOp.ClusterStatus`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ClusterId` | `guid` | Cluster identity. |
| 2 | `ClusterName` | `string` | Cluster name. |
| 3 | `Control` | `GroupStatusData` | Control group status. |
| 4 | `Nodes` | `list<NodeEntry>` | Members. |
| 5 | `Volumes` | `list<string>` | Volume names. |
| 6 | `PlacementEpoch` | `varuint` | Current placement epoch. |
| 7 | `ActiveFeatures` | `set<string>` | Features active cluster-wide. |
| 8 | — | `extensions` | Terminal extension block. |

#### `AdminCreatePrincipalBody`

Request of `AdminOp.CreatePrincipal`. The server stores only a salted PBKDF2 verifier.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Principal` | `string` | Principal name. |
| 2 | `Secret` | `string` | Initial secret (never logged). |
| 3 | `ClusterAdmin` | `bool` | Grant cluster administration. |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminDefinePolicyBody`

Request of `AdminOp.DefinePolicy`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | `PolicyName` | `string` | Policy name. |
| 3 | `Policy` | `StoragePolicy` | Policy definition. |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminEmptyBody`

Empty administrative body.

No fields; the encoding is only the extension block.

#### `AdminExportRecoveryKeyBody`

Request of `AdminOp.ExportRecoveryKey`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Passphrase` | `string` | Passphrase the escrow is wrapped under (never logged). |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminGroupListBody`

Result of `AdminOp.ListGroups`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Groups` | `list<GroupInfoData>` | Groups ordered by name (ordinal). |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminGroupMemberBody`

Request of `AdminOp.AddGroupMember` and `AdminOp.RemoveGroupMember`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Group` | `string` | Group name. |
| 2 | `Principal` | `string` | Principal name. |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminGroupNameBody`

Request of `AdminOp.CreateGroup` and `AdminOp.DeleteGroup`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Group` | `string` | Group name (a valid `DfsIdentity` name). |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminGrantBody`

Request of `AdminOp.Grant` and `AdminOp.Revoke`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | `Principal` | `string` | Principal name. |
| 3 | `Permissions` | `enum DfsPermissions (raw)` | Permissions to grant or revoke (unknown bits are rejected by the server). |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminKeyRotationBody`

Result of `AdminOp.RotateDataKey`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | `KeyVersion` | `varuint` | New data-key version. |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminNameListBody`

A list of names or lines (`AdminOp.ListVolumes`, `AdminOp.AuditLog`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Names` | `list<string>` | Names or audit lines. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminNodeListBody`

Result of `AdminOp.ListNodes`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Nodes` | `list<NodeEntry>` | Members ordered by name. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminNodeNameBody`

Request naming one node (`AdminOp.DrainNode`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Node name. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminPolicyMigrationBody`

Result of `AdminOp.PolicyMigrationStatus`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | `Target` | `StoragePolicy` | Target default policy. |
| 3 | `Effective` | `bool` | True once migration has completed. |
| 4 | `PendingObjects` | `varint` | Objects still to migrate. |
| 5 | — | `extensions` | Terminal extension block. |

#### `AdminPoolListBody`

Result of `AdminOp.ListPools`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Pools` | `list<PoolInfoData>` | Pools ordered by name (ordinal), the implicit `default` pool included. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminPoolMembersBody`

Request of `AdminOp.CreatePool` and `AdminOp.SetPoolMembers`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Pool` | `string` | Pool name (`DfsPoolNames` rules; `default` is implicit and cannot be set). |
| 2 | `Nodes` | `list<string>` | Member node names (non-empty, live members). |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminPoolNameBody`

Request of `AdminOp.DeletePool` and `AdminOp.PoolStatus`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Pool` | `string` | Pool name. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminPoolStatusBody`

Result of `AdminOp.PoolStatus` (mirrors the SDK's `DfsPoolStatus`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Info` | `PoolInfoData` | The pool. |
| 2 | `Replicas` | `ReplicaHealthData` | Replica health aggregated over the pool's volumes. |
| 3 | `DrainingNodes` | `list<string>` | Member nodes being drained (ordinal order). |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminPrincipalNameBody`

Request of `AdminOp.DeletePrincipal`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Principal` | `string` | Principal name. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminRecoveryKeyBody`

Result of `AdminOp.ExportRecoveryKey`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Escrow` | `bytes` | KEK escrow wrapped under the passphrase. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminRemoveNodeBody`

Request of `AdminOp.RemoveNode`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Node name. |
| 2 | `Force` | `bool` | Remove even if the node still holds required data or votes (unsafe; audited). |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminRestoreFromBackupBody`

Request of `AdminOp.RestoreFromBackup` (mirrors the SDK's `DfsRestoreFromBackupRequest`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `BackupEndpoints` | `list<string>` | Endpoints of the backup node. |
| 2 | `BackupCredentials` | `Credentials` | Credentials for the backup node (reader of the snapshot). |
| 3 | `SourceVolume` | `string` | Source volume name on the backup node. |
| 4 | `SnapshotName` | `string` | Published snapshot to restore. |
| 5 | `NewVolumeName` | `string` | Name of the new live volume. |
| 6 | `RecoveryKey` | `bytes` | Recovery key material when the backup node needs it (empty = none). |
| 7 | — | `extensions` | Terminal extension block. |

#### `AdminSetDefaultPolicyBody`

Request of `AdminOp.SetDefaultPolicy`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | `Policy` | `StoragePolicy` | New default policy (validated; impossible policies are rejected with InvalidPolicy). |
| 3 | `MigrateExisting` | `bool` | True to migrate existing objects (reported effective only after migration). |
| 4 | — | `extensions` | Terminal extension block. |

#### `AdminSetFeatureBody`

Request of `AdminOp.SetFeature`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Feature` | `string` | Feature name. |
| 2 | `Enabled` | `bool` | True to activate (only once every member supports it, FMT-009). |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminSetNodeLabelsBody`

Request of `AdminOp.SetNodeLabels`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Node name. |
| 2 | `Labels` | `Labels` | New labels (bumps the placement epoch). |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminStartScrubBody`

Request of `AdminOp.StartScrub`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Node` | `string?` | Node to scrub; null = all nodes. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminVolumeInfoBody`

Volume description returned by `AdminOp.CreateVolume` and `AdminOp.RestoreFromBackup`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeEntry` | Volume entry. |
| 2 | `LastCommit` | `CommitToken` | Last commit of the volume. |
| 3 | — | `extensions` | Terminal extension block. |

#### `AdminVolumeNameBody`

Request naming one volume.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name. |
| 2 | — | `extensions` | Terminal extension block. |

#### `AdminVolumeSpecBody`

Request of `AdminOp.CreateVolume` (mirrors the SDK's `DfsVolumeSpec`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Volume name. |
| 2 | `Metadata` | `MetadataPolicy` | Metadata (voter) policy. |
| 3 | `DefaultPolicy` | `StoragePolicy` | Default storage policy. |
| 4 | `NamedPolicies` | `map<string, StoragePolicy>` | Named storage policies. |
| 5 | `InlineThreshold` | `varint32` | Inline threshold in bytes. |
| 6 | `Limits` | `VolumeLimits` | Volume limits. |
| 7 | `StoragePool` | `string` | Storage pool the volume is bound to (Addendum A, POOL-02; `default` when unspecified). |
| 8 | `RootSecurity` | `SecurityDescriptor?` | Root security descriptor; null = `DfsSecurityDescriptor.DefaultRoot(creator)` (ACL-06). |
| 9 | `EnforceTraverseChecking` | `bool` | Require Traverse on every ancestor directory (ACL-05). |
| 10 | — | `extensions` | Terminal extension block. |

#### `AdminVolumeStatusBody`

Result of `AdminOp.VolumeStatus`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `VolumeEntry` | Volume entry. |
| 2 | `LastCommit` | `CommitToken` | Last commit of the volume. |
| 3 | `Group` | `GroupStatusData` | Volume group status. |
| 4 | `Replicas` | `ReplicaHealthData` | Replica health. |
| 5 | `Backups` | `list<BackupStatusData>` | Backup target status. |
| 6 | — | `extensions` | Terminal extension block. |

#### `BackupBudgetsData`

Backup resource budgets (mirrors the SDK's `DfsBackupBudgets`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `MaxPinAge` | `duration` | Maximum age of a transfer pin before the lineage is abandoned. |
| 2 | `MaxPinnedBytes` | `varint` | Maximum source bytes held by transfer pins. |
| 3 | `MaxBytesPerSecond` | `varint` | Transfer bandwidth limit. |
| 4 | `MaxConcurrentObjects` | `varint32` | Concurrent object transfers. |
| 5 | — | `extensions` | Terminal extension block. |

#### `BackupRestoreDescriptor`

Description of a published backup snapshot for restore.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Snapshot` | `SnapshotInfoData` | The snapshot. |
| 2 | `SourceClusterId` | `guid` | Source cluster. |
| 3 | `SourceVolumeId` | `VolumeId` | Source volume identity. |
| 4 | `SourceEpoch` | `varuint` | Source volume epoch. |
| 5 | `ManifestLength` | `varint` | Manifest length in bytes. |
| 6 | `ManifestChecksum` | `u128` | XxHash128 of the manifest. |
| 7 | `ObjectCount` | `varint` | Objects in the dependency closure. |
| 8 | `TotalObjectBytes` | `varint` | Total plaintext bytes of those objects. |
| 9 | — | `extensions` | Terminal extension block. |

#### `BackupRetainedSnapshotData`

One snapshot retained by a backup node.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Snapshot` | `SnapshotId` | Snapshot identity. |
| 2 | `SnapshotName` | `string` | Snapshot name. |
| 3 | `Covered` | `CommitToken` | Source commit covered. |
| 4 | `Retention` | `Retention` | Retention enforced by the backup node. |
| 5 | `Published` | `bool` | True when the snapshot is published (complete and readable). |
| 6 | — | `extensions` | Terminal extension block. |

#### `BackupScheduleData`

Backup schedule (mirrors the SDK's `DfsBackupSchedule`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Interval` | `duration?` | Periodic synchronization interval; null = none. |
| 2 | `OnEverySnapshot` | `bool` | Synchronize every new snapshot. |
| 3 | — | `extensions` | Terminal extension block. |

#### `BackupStatusData`

Status of one backup target (mirrors the SDK's `DfsBackupStatus`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Target` | `string` | Target name. |
| 2 | `State` | `enum WireBackupState` | Transfer state. |
| 3 | `ReceivedBytes` | `varint` | Bytes received by the target in the current transfer. |
| 4 | `DurableBoundary` | `CommitToken` | Highest boundary whose objects and manifest are durable on the target. |
| 5 | `PublishedBoundary` | `CommitToken` | Highest boundary published (readable) on the target. |
| 6 | `LatestPublished` | `BackupCheckpoint?` | Latest published checkpoint. |
| 7 | `RecoveryPointAge` | `duration?` | Age of the latest published recovery point (leader clock). |
| 8 | `ReseedRequired` | `bool` | True when the incremental lineage was abandoned. |
| 9 | `PinnedSourceBytes` | `varint` | Source bytes held by transfer pins for this target. |
| 10 | `LastError` | `string?` | Last transfer error. |
| 11 | `LineageId` | `guid` | Current transfer lineage. |
| 12 | — | `extensions` | Terminal extension block. |

#### `BackupTransferKey`

Identity of one snapshot transfer to a backup node.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `SourceClusterId` | `guid` | Source cluster. |
| 2 | `Volume` | `VolumeId` | Source volume. |
| 3 | `Epoch` | `varuint` | Source volume epoch (older epochs are rejected, BAK-026). |
| 4 | `LineageId` | `guid` | Incremental transfer lineage. |
| 5 | `Snapshot` | `SnapshotId` | Snapshot being transferred. |
| 6 | — | `extensions` | Terminal extension block. |

#### `ClusterMapData`

The cluster map: identity, configuration position, members, volumes and capabilities. Served by every live node from its applied control state (control learners have it too); a backup node serves a map containing only itself and the volumes it holds published snapshots of.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `ClusterId` | `guid` | Cluster identity. |
| 2 | `ClusterName` | `string` | Cluster name. |
| 3 | `ConfigIndex` | `varuint` | Control-group applied index the map was built from (monotonic; used for change detection). |
| 4 | `PlacementEpoch` | `varuint` | Current placement epoch (versioned placement map, ARCHITECTURE §6). |
| 5 | `Nodes` | `list<NodeEntry>` | Members, ordered by node name (ordinal). |
| 6 | `Volumes` | `list<VolumeEntry>` | Volumes, ordered by volume name (ordinal). |
| 7 | `ControlLeader` | `NodeId?` | Last known control leader (advisory; administrative requests go there). |
| 8 | `Capabilities` | `ServerCapabilities` | Cluster-wide capabilities and limits. |
| 9 | — | `extensions` | Terminal extension block. |

#### `DataCapability`

Short-lived, MAC-protected authorization for data-plane requests (ARCHITECTURE §5.4, §11). The volume leader issues it; storage nodes verify it without contacting the leader. Object ids alone grant nothing (SEC-004): a `DataCapabilityKind.Staging` capability allows `PutObject` into one open staging session, a `DataCapabilityKind.Read` capability allows `GetObject` on an explicit set of objects. Clients treat the encoded token (`Encode`) as opaque bytes.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Kind` | `enum DataCapabilityKind` | Capability scope. |
| 2 | `ClusterId` | `guid` | Issuing cluster. |
| 3 | `Volume` | `VolumeId` | Volume the objects belong to. |
| 4 | `VolumeEpoch` | `varuint` | Volume epoch at issue time (a capability of an older epoch is rejected, SEC-007). |
| 5 | `Principal` | `string` | Principal the capability was issued to (verifiers compare it with the connection's principal). |
| 6 | `Staging` | `StagingId` | Staging session (`DataCapabilityKind.Staging`); empty for read capabilities. |
| 7 | `Objects` | `list<ObjectId>` | Readable objects in strictly ascending order (`DataCapabilityKind.Read`); empty for staging capabilities. |
| 8 | `ExpiresUtc` | `timestamp` | Expiry on the issuer's wall clock; verifiers reject expired capabilities using their own clock. |
| 9 | `Mac` | `bytes[0|32]` | HMAC-SHA256 with the cluster capability key over `GetMacInput`; empty before `Sign`. |
| 10 | — | `extensions` | Terminal extension block. |

#### `DirectoryEntryData`

One entry of a directory listing page.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Entry name (NFC, one component). |
| 2 | `Kind` | `enum WireEntryKind` | Entry kind. |
| 3 | `Inode` | `InodeId` | Inode identity. |
| 4 | `Revision` | `varuint` | Revision of the entry's inode. |
| 5 | `Length` | `varint` | File length (0 for directories). |
| 6 | `ModifiedUtc` | `timestamp` | Last modification time. |
| 7 | — | `extensions` | Terminal extension block. |

#### `ErrorDetailData`

Structured details of an `ErrorReplyMessage`, so the typed exception the SDK raises carries the same properties the server's exception had (FMT-011, API.md §A.2). Fields that do not apply to the reply's code are zero/null.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `RequiredCopies` | `varint32` | Placement: durable copies (distinct hosts) the policy requires. |
| 2 | `AchievedCopies` | `varint32` | Placement: durable copies achieved. |
| 3 | `RequiredZones` | `varint32` | Placement: distinct (region, zone) pairs the policy requires. |
| 4 | `AchievedZones` | `varint32` | Placement: distinct zones achieved. |
| 5 | `LimitName` | `string?` | Limit: name of the exceeded limit (for example `MaxPublishBytes`). |
| 6 | `Limit` | `varint` | Limit: the configured limit. |
| 7 | `Requested` | `varint` | Limit: the requested amount. |
| 8 | `Object` | `ObjectId?` | Integrity: the corrupt or missing object, when known. |
| 9 | `AdapterId` | `string?` | Adapter failure: the adapter id. |
| 10 | `Phase` | `string?` | Adapter failure: the failing phase. |
| 11 | — | `extensions` | Terminal extension block. |

#### `ExtentData`

One extent of a committed file revision as returned by reads: logical range `[Offset, Offset + Length)` maps to object bytes `[ObjectOffset, ObjectOffset + Length)`. Ranges below the file length without an extent are holes (zeroes).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Offset` | `varint` | Logical file offset. |
| 2 | `Length` | `varint` | Logical length. |
| 3 | `Object` | `ObjectId` | Object holding the bytes. |
| 4 | `ObjectOffset` | `varint` | Offset of the extent's first byte inside the object. |
| 5 | `ObjectLength` | `varint` | Full plaintext length of the object (needed to verify `Checksum`). |
| 6 | `Checksum` | `u128` | XxHash128 of the whole object plaintext. |
| 7 | `Encoding` | `u8` | Object encoding (`ObjectEncodings`). |
| 8 | `PlacementEpoch` | `varuint` | Placement epoch the object was placed under (HRW targets are computed from it). |
| 9 | `Locations` | `list<NodeId>` | Nodes known to hold verified copies (tried first), most recent knowledge first. |
| 10 | — | `extensions` | Terminal extension block. |

#### `FileInfoData`

Metadata of a file or directory (FSD NS-04), used by open, get-info and create-directory replies.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Path` | `string` | Normalized path the entry was resolved through (empty for a detached inode reached through a handle). |
| 2 | `Kind` | `enum WireEntryKind` | Entry kind. |
| 3 | `Inode` | `InodeId` | Inode identity. |
| 4 | `Revision` | `varuint` | Revision (log index of the last modification). For a file: the last change of its default stream or of its own metadata (owner, ACL, storage policy, name/parent) — writes to named streams do not change it (ADS-011); for a named stream (`IsStream`): the stream's last change; for a directory: the last change of its entries or metadata. |
| 5 | `Length` | `varint` | File length (0 for directories). |
| 6 | `CreatedUtc` | `timestamp` | Creation time (leader wall clock carried in the log entry). |
| 7 | `ModifiedUtc` | `timestamp` | Last modification time (leader wall clock carried in the log entry). |
| 8 | `Owner` | `string` | Owning principal. |
| 9 | `OwnerOnly` | `bool` | Inode ACL: only the owner may access. |
| 10 | `ReadOnlyForOthers` | `bool` | Inode ACL: principals other than the owner may only read. |
| 11 | `StoragePolicy` | `string` | Effective storage policy name (empty = the volume default). |
| 12 | `IsInline` | `bool` | True when the contents are stored inline in metadata. |
| 13 | `IsSparse` | `bool` | True when the file has holes (ranges without extents below its length). |
| 14 | `ExtentCount` | `varint32` | Number of extents (0 for inline files and directories). |
| 15 | `EntryCount` | `varint32` | Number of children (directories only). |
| 16 | `WriterGeneration` | `varuint` | Current writer fencing generation of the inode (files only). |
| 17 | `IsStream` | `bool` | True when the entry describes a named stream (`file:stream` path); `Length`, `Revision`, `IsInline`, `IsSparse` and `ExtentCount` then describe that stream, the other fields its file. |
| 18 | `StreamName` | `string?` | Name of the described named stream; null for files and directories. |
| 19 | `StreamCount` | `varint32` | Number of named streams of the inode (the default stream is not counted). |
| 20 | — | `extensions` | Terminal extension block. |

#### `GroupInfoData`

One principal group (mirrors the SDK's `DfsGroupInfo`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Group name. |
| 2 | `Members` | `list<string>` | Member principals (ordinal order). |
| 3 | — | `extensions` | Terminal extension block. |

#### `GroupStatusData`

Status of one Raft group (control or volume).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Group` | `string` | Group name (control, or the volume name). |
| 2 | `Voters` | `list<string>` | Voter node names. |
| 3 | `Learners` | `list<string>` | Learner node names. |
| 4 | `Leader` | `string?` | Leader node name, if any. |
| 5 | `Term` | `varuint` | Current term. |
| 6 | `CommitIndex` | `varuint` | Commit index. |
| 7 | `AppliedIndex` | `varuint` | Applied index of the answering replica. |
| 8 | `HasQuorum` | `bool` | True when a majority of configured voters is reachable from the leader. |
| 9 | — | `extensions` | Terminal extension block. |

#### `HandlePinUpdate`

Advances the GC-protection index of one live-read handle (part of `KeepAliveMessage`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Handle` | `HandleId` | Handle whose pin moves. |
| 2 | `PinIndex` | `varuint` | New pin index: the client no longer needs objects of revisions older than this index. |
| 3 | — | `extensions` | Terminal extension block. |

#### `InventoryItem`

One stored object reported in an `InventoryReportMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Object` | `ObjectId` | Stored object. |
| 2 | `Staging` | `StagingId` | Staging session it was uploaded under. |
| 3 | `Length` | `varint` | Plaintext length. |
| 4 | `Checksum` | `u128` | XxHash128 of the plaintext. |
| 5 | `Encoding` | `u8` | Object encoding. |
| 6 | — | `extensions` | Terminal extension block. |

#### `NodeEntry`

One member of the cluster map (live node or registered backup node).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Id` | `NodeId` | Node identity. |
| 2 | `Name` | `string` | Unique node name. |
| 3 | `Labels` | `Labels` | Failure-domain labels (copies count distinct hosts, zones count distinct region/zone pairs). |
| 4 | `Endpoints` | `list<string>` | Endpoints the node listens on (`quic://host:port` or `mem://name`), preferred first. |
| 5 | `Status` | `enum WireNodeStatus` | Membership status. |
| 6 | `Incarnation` | `varuint` | Latest registered incarnation; receipts and Raft traffic with an older incarnation are rejected. |
| 7 | `IsBackup` | `bool` | True for a backup node (never a data copy, never a Raft member). |
| 8 | `IsControlVoter` | `bool` | True when the node is a voter of the control group. |
| 9 | `IsReachable` | `bool` | Control leader's current view of reachability (advisory). |
| 10 | `PlacementEligible` | `bool` | True when new object placements may target the node (active, not draining, not full). |
| 11 | `CapacityBytes` | `varint` | Configured capacity. |
| 12 | `UsedBytes` | `varint` | Last reported used bytes (advisory; free = capacity − used). |
| 13 | `Pools` | `set<string>` | Named storage pools the node belongs to (Addendum A, POOL-02; the implicit `default` pool holds every member and is not listed). Clients place objects of a policy with `DfsStoragePolicy.Pool` (or the volume's `StoragePool`) on the members of that pool only (trailing field). |
| 14 | — | `extensions` | Terminal extension block. |

#### `ObjectCopy`

One server-side object copy of a `CopyObjectsMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `From` | `ObjectId` | Source object (owned by the source volume). |
| 2 | `To` | `ObjectId` | New object id (owned by the target volume). |
| 3 | — | `extensions` | Terminal extension block. |

#### `ObjectLocation`

Answer for one object of a `LocateObjectMessage`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Object` | `ObjectId` | Object asked about. |
| 2 | `Present` | `bool` | True when this node holds a verified copy. |
| 3 | `Length` | `varint` | Plaintext length of the held copy (0 when absent). |
| 4 | `Checksum` | `u128` | Checksum of the held copy (zero when absent). |
| 5 | `Staging` | `StagingId` | Staging session the copy was stored under (empty when absent). |
| 6 | — | `extensions` | Terminal extension block. |

#### `ObjectReceipt`

Signed durability receipt for one stored object (ARCHITECTURE §6). A storage node issues it only after the object record and its segment flush completed; the volume leader validates every receipt (MAC, binding to the committed change, node incarnation current, placement epoch valid, distinct hosts/zones) before proposing a publication.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Object` | `ObjectId` | Stored object. |
| 2 | `Volume` | `VolumeId` | Volume the object was stored for. |
| 3 | `Staging` | `StagingId` | Staging session the object was uploaded under (decides GC eligibility). |
| 4 | `Checksum` | `u128` | XxHash128 of the object's plaintext. |
| 5 | `Length` | `varint` | Plaintext length of the object. |
| 6 | `Encoding` | `u8` | Object encoding (`ObjectEncodings`). |
| 7 | `Node` | `NodeId` | Storage node that holds the durable copy. |
| 8 | `Incarnation` | `varuint` | Incarnation of `Node` that issued the receipt. |
| 9 | `PlacementEpoch` | `varuint` | Placement epoch the upload targeted. |
| 10 | `DurableAtTicks` | `varint` | Storing node's clock (UTC ticks) when durability was reached (diagnostic; never used for ordering). |
| 11 | `Mac` | `bytes[32]` | HMAC-SHA256 with the cluster receipt key over `GetMacInput` (exactly `ProtocolInfo.MacSize` bytes). |
| 12 | — | `extensions` | Terminal extension block. |

#### `ObjectStagingRef`

A stored copy identified by object and staging session: the unit of garbage-collection decisions (ARCHITECTURE §8, GC-005). Deletability is decided for an (object, staging) pair whose staging session is closed, so a storage node deletes a copy only while its stored record still carries `Staging`; a copy that a later upload re-bound to another (possibly open, possibly committed) staging session is kept.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Object` | `ObjectId` | Stored object. |
| 2 | `Staging` | `StagingId` | Staging session the decision was taken for. |
| 3 | — | `extensions` | Terminal extension block. |

#### `PoolInfoData`

One storage pool (mirrors the SDK's `DfsPoolInfo`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Pool name. |
| 2 | `Nodes` | `list<string>` | Member node names (ordinal order). |
| 3 | `Volumes` | `list<string>` | Names of volumes bound to the pool (ordinal order). |
| 4 | `CapacityBytes` | `varint` | Sum of the members' configured capacities. |
| 5 | `UsedBytes` | `varint` | Sum of the members' last reported usage (advisory). |
| 6 | — | `extensions` | Terminal extension block. |

#### `RaftEnvelope`

One Raft message addressed to a group (element of `RaftBatchMessage`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Group` | `string` | Group name (`ProtocolInfo.ControlGroup` or `ProtocolInfo.VolumeGroup`). |
| 2 | `Message` | `RaftWireMessage` | The message. |
| 3 | — | `extensions` | Terminal extension block. |

#### `RaftWireEntry`

One Raft log entry on the wire (mirrors `Tedd.Tdfs.Consensus.RaftEntry`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Index` | `varuint` | Log index. |
| 2 | `Term` | `varuint` | Term of the entry. |
| 3 | `Kind` | `u8` | Entry kind as the Consensus `RaftEntryKind` byte (0 Normal, 1 ConfChange, 2 Noop); validated by the receiver. |
| 4 | `Data` | `bytes` | Entry payload. |
| 5 | — | `extensions` | Terminal extension block. |

#### `RaftWireMessage`

A Raft message in a neutral, flat layout (in the style of etcd raftpb.Message) so Core does not depend on the Consensus project; the node runtime maps each Consensus message record to and from it. All fields are always encoded; fields not used by `Kind` are zero/empty. Field use per kind: VoteRequest — LogIndex/LogTerm = last log index/term, PreVote, LeadershipTransfer; VoteResponse — PreVote, Reject; AppendEntries — LogIndex/LogTerm = previous index/term, Entries, Commit, ReadContext; AppendResponse — Reject, LogIndex = match index, RejectHint, RejectHintTerm, ReadContext; InstallSnapshot — Snapshot, Offset, Data, Done; InstallSnapshotResponse — LogIndex = snapshot index, Offset = next offset, Reject; TimeoutNow — none; Heartbeat — Commit, ReadContext; HeartbeatResponse — ReadContext.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Kind` | `enum RaftWireKind` | Message kind. |
| 2 | `From` | `NodeId` | Sender. |
| 3 | `To` | `NodeId` | Recipient. |
| 4 | `Term` | `varuint` | Sender term (for PreVote requests: the term the sender would campaign in). |
| 5 | `LogIndex` | `varuint` | Log index field (meaning depends on the kind). |
| 6 | `LogTerm` | `varuint` | Log term field (meaning depends on the kind). |
| 7 | `Commit` | `varuint` | Leader commit index. |
| 8 | `ReadContext` | `varuint` | ReadIndex context being confirmed/echoed (0 = none). |
| 9 | `RejectHint` | `varuint` | Fast-backtrack probe index (AppendResponse rejections). |
| 10 | `RejectHintTerm` | `varuint` | Follower term at `RejectHint` (0 when unknown). |
| 11 | `PreVote` | `bool` | PreVote probe/response. |
| 12 | `LeadershipTransfer` | `bool` | Campaign triggered by TimeoutNow. |
| 13 | `Reject` | `bool` | Rejection flag. |
| 14 | `Entries` | `list<RaftWireEntry>` | Entries to append. |
| 15 | `Snapshot` | `RaftWireSnapshotMeta?` | Snapshot metadata (InstallSnapshot). |
| 16 | `Offset` | `varint` | Snapshot byte offset (chunk offset or next expected offset). |
| 17 | `Data` | `bytes` | Snapshot chunk bytes. |
| 18 | `Done` | `bool` | Final snapshot chunk. |
| 19 | — | `extensions` | Terminal extension block. |

#### `RaftWireSnapshotMeta`

Snapshot metadata on the wire (mirrors `RaftSnapshotMeta`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Index` | `varuint` | Last log index covered by the snapshot. |
| 2 | `Term` | `varuint` | Term of that index. |
| 3 | `Voters` | `list<NodeId>` | Voters of the configuration at the snapshot. |
| 4 | `Learners` | `list<NodeId>` | Learners of the configuration at the snapshot. |
| 5 | — | `extensions` | Terminal extension block. |

#### `RangeChange`

One change of a publication (`CommitFile`, `WriteAll`, `Append`). Every field is always encoded; fields irrelevant to `Kind` must be zero/empty.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Offset` | `varint` | Logical offset (absolute, except in `Append` where it is relative to the old end of file). |
| 2 | `Length` | `varint` | Logical length of the changed range (> 0). |
| 3 | `Kind` | `enum RangeChangeKind` | Change kind. |
| 4 | `Object` | `ObjectId` | Object holding the bytes (`RangeChangeKind.Data`). |
| 5 | `ObjectOffset` | `varint` | Offset of the range's first byte inside the object (`RangeChangeKind.Data`). |
| 6 | `ObjectLength` | `varint` | Full plaintext length of the object (`RangeChangeKind.Data`). |
| 7 | `Checksum` | `u128` | XxHash128 of the whole object plaintext (`RangeChangeKind.Data`). |
| 8 | `Encoding` | `u8` | Object encoding (`RangeChangeKind.Data`). |
| 9 | `InlineData` | `bytes` | The bytes of the range (`RangeChangeKind.Inline`; length must equal `Length`). |
| 10 | — | `extensions` | Terminal extension block. |

#### `ReplicaHealthData`

Replica health of a volume (mirrors the SDK's `DfsReplicaHealth`).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Objects` | `varint` | Referenced objects. |
| 2 | `UnderReplicatedObjects` | `varint` | Objects with fewer verified copies than the target. |
| 3 | `ZoneDeficitObjects` | `varint` | Objects in fewer zones than the target. |
| 4 | `MissingObjects` | `varint` | Objects with no verified copy. |
| 5 | `PlacementViolations` | `varint` | Copies violating placement rules. |
| 6 | `RepairBacklogBytes` | `varint` | Bytes waiting for repair. |
| 7 | `PendingGcObjects` | `varint` | Objects waiting for garbage collection. |
| 8 | `StagedBytes` | `varint` | Bytes in open staging sessions. |
| 9 | `PinnedBytes` | `varint` | Bytes protected only by pins. |
| 10 | — | `extensions` | Terminal extension block. |

#### `RequestHeader`

Common header of every volume-scoped request (namespace, files, sessions, transactions, snapshots, backup administration). It addresses the volume, binds the request to an authority (volume id and epoch), and carries the operation identity used for idempotency (FSD §13).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Volume` | `string` | Volume name (the server resolves it through the control state). |
| 2 | `ExpectedVolumeId` | `VolumeId` | Volume identity the caller believes it addresses; `VolumeId` empty = any. A mismatch (for example after the name was deleted and re-created) is rejected with `DfsErrorCode.EpochMismatch` so handles, sessions and tokens of another volume are never applied (PATH-014, RST-008). |
| 3 | `ExpectedVolumeEpoch` | `varuint` | Volume epoch the caller believes is current; 0 = any. Mismatch → `DfsErrorCode.EpochMismatch`. |
| 4 | `Session` | `varuint` | The caller's own session on this volume (0 = none). Handles, pins and staging sessions created by the request are owned by this session and renewed by its `KeepAlive`. |
| 5 | `Operation` | `OperationId` | Stable operation id of a mutating request, assigned before the first transmission and reused by every retry (OUT-001); empty for reads. It may belong to another session of the same principal (an id persisted by a previous process). |
| 6 | `AckWatermark` | `varuint` | Sequence watermark of `Session`: the client will never retry or query any of its operations with a lower sequence, so their outcomes may be retired (ARCHITECTURE §5.2 step 4). 0 = none. |
| 7 | `Transaction` | `TransactionId?` | Transaction the request belongs to; null outside transactions. Transaction-bound requests are applied to the transaction's private working root and are not visible to anyone else before commit. |
| 8 | — | `extensions` | Terminal extension block. |

#### `SnapshotInfoData`

Description of a snapshot (live volume or published backup snapshot).

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Id` | `SnapshotId` | Snapshot identity. |
| 2 | `Name` | `string` | Snapshot name (unique per volume). |
| 3 | `Volume` | `string` | Volume name. |
| 4 | `Boundary` | `CommitToken` | Commit boundary captured by the snapshot (the entry that created it). |
| 5 | `Label` | `enum DfsConsistencyLabel` | Consistency label. |
| 6 | `Adapters` | `list<AdapterRecordData>` | Adapters whose prepare succeeded (required for `DfsConsistencyLabel.ApplicationConsistent`). |
| 7 | `CreatedUtc` | `timestamp` | Creation time (leader wall clock carried in the log entry). |
| 8 | `Retention` | `Retention` | Retention (expiry and hold). |
| 9 | — | `extensions` | Terminal extension block. |

#### `StreamInfoData`

One named stream of a file or directory (mirrors the SDK's `DfsStreamInfo`), used by `ListStreamsResult`.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Name` | `string` | Stream name (NFC). |
| 2 | `Length` | `varint` | Stream length. |
| 3 | `Revision` | `varuint` | Revision (log index) of the stream's last modification. |
| 4 | — | `extensions` | Terminal extension block. |

#### `VolumeEntry`

One volume of the cluster map: identity, authority epoch, routing and policies.

| # | Field | Wire type | Meaning |
|---|---|---|---|
| 1 | `Id` | `VolumeId` | Volume identity. |
| 2 | `Name` | `string` | Volume name. |
| 3 | `Epoch` | `varuint` | Volume epoch (increments on restore/promotion; requests may pin it via `RequestHeader.ExpectedVolumeEpoch`). |
| 4 | `State` | `enum WireVolumeState` | Lifecycle state. |
| 5 | `Voters` | `list<NodeId>` | Voters of the volume Raft group. |
| 6 | `Learners` | `list<NodeId>` | Learners of the volume Raft group. |
| 7 | `LeaderHint` | `NodeId?` | Last known leader of the volume group (advisory). |
| 8 | `Metadata` | `MetadataPolicy` | Metadata (voter) policy. |
| 9 | `DefaultPolicy` | `StoragePolicy` | Default storage policy for new files. |
| 10 | `NamedPolicies` | `map<string, StoragePolicy>` | Named alternative storage policies (ordinal names). |
| 11 | `InlineThreshold` | `varint32` | Largest file length stored inline in metadata. |
| 12 | `Limits` | `VolumeLimits` | Volume limits. |
| 13 | `StoragePool` | `string` | Storage pool the volume is bound to (Addendum A). |
| 14 | `EnforceTraverseChecking` | `bool` | True when the volume enforces Traverse on ancestor directories. |
| 15 | — | `extensions` | Terminal extension block. |


## 11. Errors

### 11.1 Error replies

Any request may be answered with `ErrorReply { Code, Message, OperationId?, Retryable, LeaderHint?, TransactionId?,
Detail? }` (`ErrorCodec`, FMT-011). Codes travel verbatim (codes unknown to an older receiver are preserved); the SDK
maps them with `ErrorReplyMessage.ToException` (`DfsErrors.ToException` plus the transported transaction id, leader hint
and `Detail`), so an unknown commit outcome can never degrade into not-found or cancellation, and typed properties
(`DfsPlacementUnavailableException.RequiredCopies`/`AchievedCopies`/`RequiredZones`/`AchievedZones`,
`DfsLimitExceededException.LimitName`/`Limit`/`Requested`, `DfsIntegrityException.ObjectId`,
`DfsSnapshotAdapterException.AdapterId`/`Phase`) survive the wire (`ErrorReplyMessage.FromException` fills `Detail`).
Messages never contain payload bytes or secrets.

| Situation | Code | Retryable | Notes |
|---|---|---|---|
| Malformed frame, unknown type, trailing bytes, bad varint/UTF-8/length, unsorted set | `ProtocolError` | no | The server answers when the transport can correlate the request, otherwise (or on repetition, or during the handshake) it closes the connection. |
| Unknown required extension; no common protocol version; unknown `AdminOp`; optional request not implemented (`TxOp`) | `Unsupported` | no | FMT-002. |
| Wrong cluster | `ClusterMismatch` | no | Handshake only. |
| Bad or missing credentials, unauthenticated request | `AuthenticationFailed` | no | Connection closed. |
| Insufficient permission, node-only message from a client, invalid capability | `AccessDenied` | no | Mapped to `UnauthorizedAccessException`. |
| Not the leader of the addressed group | `NotLeader` | yes | `LeaderHint` = leader endpoint when known. |
| No quorum / node unavailable / placement impossible now / capacity | `QuorumUnavailable` / `NodeUnavailable` / `PlacementUnavailable` / `CapacityExhausted` | yes | Distinct availability errors (FSD §13). |
| Volume id/epoch differs from the header's expectation | `EpochMismatch` | no | Refresh the cluster map. |
| Stale node incarnation | `StaleIncarnation` | no | |
| Operation id outside the retained window / reused with other arguments or principal | `OperationExpired` / `OperationIdMismatch` | no | Never executed as a new operation. |
| Outcome may have committed (reported by a forwarding node or the SDK) | `OutcomeUnknown` | no | Carries the operation and transaction id; resolve with `QueryOperation`. |
| Namespace errors | `FileNotFound`, `DirectoryNotFound`, `AlreadyExists`, `DirectoryNotEmpty`, `NotADirectory`, `IsADirectory`, `InvalidPath`, `InvalidArgument`, `CrossVolume` | no | Standard .NET exceptions on the SDK side. |
| Sharing / writer conflicts | `SharingViolation`, `WriterConflict` | yes | Never waits. |
| Fencing | `LeaseLost`, `InvalidHandle`, `PinExpired`, `ContinuationExpired` | no | The SDK faults the stream / enumeration. |
| Transactions | `TransactionConflict`, `TransactionAborted` | yes (application decides) | Carry the transaction id. |
| Read-only target (snapshot view, backup node) | `ReadOnly` | no | BAK-015. |
| Snapshots / retention / backup | `SnapshotNotFound`, `RetentionProtected`, `BackupTargetNotFound`, `RecoveryKeyMissing` | no | |
| Integrity failure (corrupt or missing object, checksum mismatch, conflicting re-upload) | `Integrity` | no (try another replica) | Never zero-filled. |
| Limits | `LimitExceeded` | no | Nothing partially published. |

### 11.2 Connection-level rules

* A peer never acts on a reply whose type is not `ErrorReply` or the request's reply type (`ProtocolError`).
* A server that receives a reply type, a frame after a failed handshake, or a node-only message on a client
  connection treats it as a protocol/authorization error.
* Decoding happens before any side effect: a request that fails to decode has no effect.

## 12. Versioning and compatibility

* `ProtocolInfo.CurrentVersion = 1`, `MinSupportedVersion = 1`, negotiated per connection (§3).
* Message type values, field order and field types of version 1 are frozen. New fields are extensions (§2.5); new
  message types get new values (unknown types are rejected with `ProtocolError`, so senders only use them when the
  peer advertises the corresponding feature).
* Features (`HelloChallenge.Features`, `DfsServerCapabilities.Features`) activate cluster-wide only when every member
  supports them (`AdminOp.SetFeature`, FMT-009); peers only send required extensions or new message types belonging
  to features active on the connection.
* Golden vectors in `tests/Tedd.Tdfs.Tests.Unit/Internal/Protocol/GoldenVectorTests.cs` pin the layout; a change to one
  of them is a wire-format break.

## 13. Design decisions beyond ARCHITECTURE.md

1. **One record per message type**, named `{MessageType}Message`, all registered in
   `MessageCodec.Registrations.cs`; nested records implement `IWireRecord<TSelf>` and end with their own extension
   block, so list elements can evolve too. Unknown optional extensions are preserved on decode and re-emitted on
   encode (forwarding nodes never strip them).
2. **Canonical encoding** everywhere (minimal varints, sorted sets/maps, exact fixed sizes, encoders refuse undefined
   strict enums), giving `encode(decode(x)) == x` and stable payload hashes.
3. **Strict vs raw enums**: protocol enums are strict; enums whose invalid values are *semantic* errors
   (`FileMode`/`FileAccess`/`FileShare`/`FileOptions`, permissions, error codes, voter placement, entry-kind filter,
   admin op) are transported raw so the server can answer `InvalidArgument`/`InvalidPolicy`/`Unsupported`
   (PATH-012, OPEN tests, AT-16) rather than a protocol error.
4. **`RequestHeader` carries `ExpectedVolumeId` in addition to the epoch**, so a deleted and re-created volume name can
   never accept the old volume's numeric session, handle, pin or continuation ids (PATH-014, RST-008).
5. **Payload identity** (`OperationPayload`) is defined on the normalized request frame, without the per-attempt
   upload artifacts of publications (receipts, staging session, random object ids), so legitimate retries from other
   sessions/processes — including ones that had to re-upload the content — match while any argument or content change
   is detected (OUT-003, OUT-006). Header-bearing requests implement `IVolumeRequest` for generic
   routing/authority/idempotency handling.
6. **Handshake** uses the SCRAM-SHA-256 key schedule for principals (the server stores no secret-equivalent) and
   label-separated HMACs for nodes, both over a transcript of the exact frames, with mutual MACs (`HandshakeAuth`).
7. **Receipts** bind volume and staging session in addition to the ARCHITECTURE fields, so a receipt can neither be
   replayed into another volume nor detach an object from the staging session whose closure governs its GC.
8. **Data capabilities** are structured, MAC-covered records (`DataCapability`) with an explicit object list for reads
   (object ids alone grant nothing) and an epoch binding; extensions are MAC-covered.
9. **Inline data is a `RangeChange` kind** (no separate inline field); converting an inline file to extents is done by
   the leader from the old inline bytes, so append never needs a read-modify-write.
10. **Truncation watermark** (`CommitFile.TruncateTo`) makes "truncate then extend in one publication" safe without
    sending zero ranges.
11. **Reads**: snapshot views read by `(SnapshotPin, Inode)` without handles, so backup nodes never grant leases;
    `GetObject` optionally returns per-unit CRC32C for verified partial reads; extents carry node-id location hints.
12. **Enumeration pins** are exposed (`ListDirectoryResult.EnumerationPin`) so slow enumerations can be renewed with
    `KeepAlive` and abandoned ones released with `CloseSnapshot`.
13. **Raft encoding** lives in Core as a neutral flat `RaftWireMessage` (§8.7); group names are keyed by volume id.
14. **`TxOp`** is defined as an optional batching envelope of complete transaction-bound request frames; ordinary
    transaction-bound requests reuse the normal request types with `Header.Transaction` set.
15. **Admin bodies** are separately encoded records inside `AdminRequest`/`AdminResult`, so the admin surface can grow
    without new message types; unknown ops decode and are answered `Unsupported`.
16. **Backup transfer** messages are keyed by an explicit `BackupTransferKey` (source cluster, volume, epoch, lineage,
    snapshot); the manifest is opaque to the protocol and owned by the backup module.
17. **Structured error details** (`ErrorReply.Detail`, `ErrorDetailData`) carry the typed exception properties API.md
    defines (placement shortfall, limit, integrity object, adapter phase) instead of flattening them into the message.
18. **Process boot ids** (`Hello`, `JoinCluster`, `RegisterIncarnation`): two processes started from a cloned disk pick
    the same incarnation, so registration is keyed by (incarnation, boot id) and the second process is fenced
    (RSY-021). Raft traffic of a newer, not yet registered incarnation is accepted, otherwise a full-cluster restart
    could never commit the registrations.
19. **GC decisions are per (object, staging session)** (`ObjectStagingRef` in `DeleteObjects` and
    `InventoryReportResult`), matching ARCHITECTURE §8, and receipts are always bound to the uploading request's
    staging session (a re-upload under another session re-binds the copy). A delete decision taken for an old pair can
    therefore never remove a copy that became reachable through a newer staging session (GC-005).
