using System.Buffers;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>
/// A deterministic state machine replicated by one Raft group (INTERNALS.md §1.1). Hosted by a <see cref="GroupRuntime"/>
/// on the node loop.
/// </summary>
/// <remarks>
/// <para>Only <see cref="Consensus.RaftEntryKind.Normal"/> entries reach <see cref="Apply"/>; configuration-change and no-op
/// entries are handled by the host (the group's applied index still advances past them, so <see cref="AppliedIndex"/> may
/// lag the group's applied index; a checkpoint taken by the host describes the state at the group's applied index).</para>
/// <para>Determinism: <see cref="Apply"/> must produce the same state and outcome on every replica for the same entry
/// sequence; it never reads clocks, randomness or node-local state (ARCHITECTURE §3.1). Time is carried inside commands.</para>
/// <para><see cref="WriteCheckpoint"/> must be a canonical serialization: replicas with the same applied history produce
/// identical bytes (the node state digest hashes it).</para>
/// </remarks>
internal interface IReplicatedStateMachine
{
    /// <summary>Group name for diagnostics ("control" or the volume name).</summary>
    string GroupName { get; }

    /// <summary>Applies one committed command. Synchronous and deterministic.</summary>
    ApplyOutcome Apply(ulong index, ulong term, ReadOnlyMemory<byte> command);

    /// <summary>Writes the state as of <see cref="AppliedIndex"/> (canonical bytes).</summary>
    void WriteCheckpoint(IBufferWriter<byte> output);

    /// <summary>Replaces the whole state with a checkpoint taken at (<paramref name="index"/>, <paramref name="term"/>).</summary>
    void RestoreCheckpoint(ReadOnlySequence<byte> data, ulong index, ulong term);

    /// <summary>Index of the last applied command (or restored checkpoint).</summary>
    ulong AppliedIndex { get; }
}

/// <summary>
/// Optional interface of a state machine that can describe a command without applying it, so fault contexts on followers
/// carry the operation kind and id (FaultSites raft.* sites).
/// </summary>
internal interface ICommandDescriber
{
    /// <summary>Describes <paramref name="command"/>; never throws (unknown commands describe as <see cref="DfsOperationKind.Unknown"/>).</summary>
    ProposalContext Describe(ReadOnlyMemory<byte> command);
}

/// <summary>Identity of a proposal for fault contexts and logs.</summary>
/// <param name="Operation">Protocol operation kind (FaultContext.Operation).</param>
/// <param name="OperationId">Operation id, when the command carries one.</param>
/// <param name="Path">Path, when meaningful.</param>
internal readonly record struct ProposalContext(DfsOperationKind Operation, DfsOperationId? OperationId = null, string? Path = null)
{
    /// <summary>No description.</summary>
    public static ProposalContext None => new(DfsOperationKind.Unknown);
}

/// <summary>Result of applying one command.</summary>
/// <param name="Code">Outcome code (<see cref="DfsErrorCode.None"/> for success).</param>
/// <param name="Message">Failure message (empty on success); never contains payloads or secrets.</param>
/// <param name="Value">State-machine specific result (node-local object; not replicated).</param>
internal sealed record ApplyOutcome(DfsErrorCode Code, string Message, object? Value)
{
    /// <summary>Log index of the applied entry (set by the host).</summary>
    public ulong Index { get; init; }

    /// <summary>Term of the applied entry (set by the host).</summary>
    public ulong Term { get; init; }

    /// <summary>True when <see cref="Code"/> is <see cref="DfsErrorCode.None"/>.</summary>
    public bool IsSuccess => Code == DfsErrorCode.None;

    /// <summary>A successful outcome.</summary>
    public static ApplyOutcome Success(object? value = null) => new(DfsErrorCode.None, string.Empty, value);

    /// <summary>A failed outcome (the command had no effect).</summary>
    public static ApplyOutcome Failure(DfsErrorCode code, string message, object? value = null) => new(code, message ?? string.Empty, value);

    /// <summary>The canonical exception for a failed outcome (<see cref="DfsErrors.ToException(DfsErrorCode, string, string?, DfsOperationId?)"/>).</summary>
    public Exception ToException() => DfsErrors.ToException(Code, Message);

    /// <summary>Returns the value cast to <typeparamref name="T"/>, or throws the outcome's exception when it failed.</summary>
    public T GetValueOrThrow<T>()
    {
        if (!IsSuccess)
            throw ToException();
        return Value is T typed ? typed : throw new InvalidOperationException($"Apply result is {Value?.GetType().Name ?? "null"}, expected {typeof(T).Name}.");
    }
}
