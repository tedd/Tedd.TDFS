using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>Context of one applied entry.</summary>
/// <param name="Index">Raft index of the entry.</param>
/// <param name="Term">Raft term of the entry.</param>
/// <param name="Command">The decoded command.</param>
internal sealed record ApplyContext(ulong Index, ulong Term, VolumeCommand Command)
{
    /// <summary>The leader wall clock carried by the command (apply's only notion of "now").</summary>
    public DateTimeOffset Now => Command.LeaderTime;

    /// <summary>The operation header (null for leader housekeeping commands).</summary>
    public OperationHeader? Op => Command.Op;
}

/// <summary>
/// Result of applying one volume command (the <see cref="Raft.ApplyOutcome.Value"/> of volume entries). For client commands
/// it carries the reply exactly as the client receives it; for a retried operation id it is the stored reply.
/// </summary>
/// <param name="Code">Outcome code.</param>
/// <param name="Message">Failure message.</param>
/// <param name="Reply">The reply message (success reply, or an error reply for failures with details); null for housekeeping.</param>
/// <param name="Value">Additional node-local result for the leader (for example the created handle or staging session).</param>
/// <param name="Replayed">True when the result is a stored outcome returned for a retried operation id.</param>
internal sealed record VolumeApplyResult(DfsErrorCode Code, string Message, IWireMessage? Reply, object? Value, bool Replayed = false)
{
    /// <summary>True on success.</summary>
    public bool IsSuccess => Code == DfsErrorCode.None;

    /// <summary>A successful result.</summary>
    public static VolumeApplyResult Ok(IWireMessage? reply, object? value = null) => new(DfsErrorCode.None, string.Empty, reply, value);

    /// <summary>A failed result (no effect).</summary>
    public static VolumeApplyResult Fail(DfsErrorCode code, string message, ErrorDetailData? detail = null)
        => new(code, message, new ErrorReplyMessage(code, message, Retryable: DfsErrors.IsRetryableByDefault(code)) { Detail = detail }, null);

    /// <summary>A failed result built from an exception (detail preserved).</summary>
    public static VolumeApplyResult Fail(Exception ex)
    {
        ErrorReplyMessage reply = ErrorReplyMessage.FromException(ex);
        return new VolumeApplyResult(reply.Code, reply.Message, reply, null);
    }

    /// <summary>The encoded reply frame (empty when there is no reply).</summary>
    public ReadOnlyMemory<byte> Frame => Reply is null ? ReadOnlyMemory<byte>.Empty : MessageCodec.Encode(Reply);
}

/// <summary>Thrown inside apply handlers to fail the command with a code (caught by the dispatcher; no partial effect).</summary>
internal sealed class ApplyFailure(DfsErrorCode code, string message, ErrorDetailData? detail = null) : Exception(message)
{
    public DfsErrorCode Code { get; } = code;

    public ErrorDetailData? Detail { get; } = detail;
}
