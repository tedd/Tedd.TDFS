namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Error reply to any request (<see cref="MessageType.ErrorReply"/>). Preserves the error code, operation and
/// transaction identity, retryability and the leader hint across the wire (FMT-011).
/// </summary>
/// <param name="Code">Canonical error code (encoded verbatim, including codes unknown to this version).</param>
/// <param name="Message">Human-readable message (never payloads or secrets).</param>
/// <param name="OperationId">Operation id of the failed request, when known.</param>
/// <param name="Retryable">True when retrying the same operation may succeed.</param>
/// <param name="LeaderHint">Endpoint of the current leader, for <see cref="DfsErrorCode.NotLeader"/>.</param>
/// <param name="TransactionId">Transaction id, for transaction failures and unknown transaction outcomes.</param>
public sealed record ErrorReplyMessage(
    DfsErrorCode Code,
    string Message,
    DfsOperationId? OperationId = null,
    bool Retryable = false,
    string? LeaderHint = null,
    DfsTransactionId? TransactionId = null) : IWireMessage<ErrorReplyMessage>
{
    /// <summary>The error message (null is stored as empty).</summary>
    public string Message { get; init; } = Message ?? string.Empty;

    /// <inheritdoc />
    public MessageType Type => MessageType.ErrorReply;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ErrorReply;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt((int)Code);
        writer.WriteString(Message);
        writer.WriteOptionalOperationId(OperationId);
        writer.WriteBool(Retryable);
        writer.WriteOptionalString(LeaderHint);
        writer.WriteOptionalOperationId(TransactionId?.Operation);
        writer.WriteNoExtensions();
    }

    /// <inheritdoc />
    public static ErrorReplyMessage Read(ref WireReader reader)
    {
        long code = reader.ReadVarInt();
        if (code is < int.MinValue or > int.MaxValue)
            throw WireReader.Malformed($"Error code {code} is out of range.");
        string message = reader.ReadString();
        DfsOperationId? op = reader.ReadOptionalOperationId();
        bool retryable = reader.ReadBool();
        string? leaderHint = reader.ReadOptionalString();
        DfsOperationId? tx = reader.ReadOptionalOperationId();
        reader.ReadExtensions();
        return new ErrorReplyMessage(
            (DfsErrorCode)(int)code,
            message,
            op,
            retryable,
            leaderHint,
            tx.HasValue ? new DfsTransactionId(tx.Value) : null);
    }

    /// <summary>Creates an error reply describing <paramref name="exception"/> using the canonical mapping.</summary>
    public static ErrorReplyMessage FromException(Exception exception, DfsOperationId? operationId = null, string? leaderHint = null)
    {
        ArgumentNullException.ThrowIfNull(exception);
        DfsTransactionId? tx = exception switch
        {
            DfsCommitOutcomeUnknownException u => u.TransactionId,
            DfsTransactionConflictException c when !c.TransactionId.IsEmpty => c.TransactionId,
            _ => null,
        };
        leaderHint ??= (exception as DfsUnavailableException)?.LeaderHint;
        return new ErrorReplyMessage(
            DfsErrors.ToCode(exception),
            WireText.Sanitize(exception.Message, ErrorCodec.MaxMessageBytes),
            operationId ?? DfsErrors.GetOperationId(exception),
            DfsErrors.IsRetryable(exception),
            leaderHint,
            tx);
    }

    /// <summary>Recreates the canonical exception (see <see cref="DfsErrors"/>) for this reply.</summary>
    /// <param name="path">Volume path the failed request addressed (sets <see cref="FileNotFoundException.FileName"/>).</param>
    public Exception ToException(string? path = null)
    {
        Exception ex = DfsErrors.ToException(Code, Message, path, OperationId, Retryable);
        switch (ex)
        {
            case DfsCommitOutcomeUnknownException unknown when TransactionId.HasValue:
                unknown.SetTransactionId(TransactionId);
                break;
            case DfsTransactionConflictException conflict when TransactionId.HasValue:
                conflict.SetTransactionId(TransactionId.Value);
                break;
            case DfsUnavailableException unavailable when LeaderHint is not null:
                unavailable.SetLeaderHint(LeaderHint);
                break;
        }

        return ex;
    }
}
