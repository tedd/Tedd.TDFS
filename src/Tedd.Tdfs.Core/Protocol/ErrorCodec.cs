using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Encodes and decodes complete <see cref="MessageType.ErrorReply"/> frames (FMT-011): error code, message,
/// operation id and retryability survive the round trip exactly, so an unknown commit outcome can never degrade into
/// a generic not-found or cancellation.
/// </summary>
public static class ErrorCodec
{
    /// <summary>Maximum UTF-8 bytes of an encoded error message; longer messages are truncated on a character boundary.</summary>
    public const int MaxMessageBytes = 16 * 1024;

    /// <summary>Encodes an error reply frame.</summary>
    /// <remarks>The message is sanitized (unpaired surrogates replaced, truncated to <see cref="MaxMessageBytes"/>) so encoding never fails.</remarks>
    public static byte[] Encode(DfsErrorCode code, string message, DfsOperationId? op, bool retryable)
        => MessageCodec.Encode(new ErrorReplyMessage(code, WireText.Sanitize(message, MaxMessageBytes), op, retryable));

    /// <summary>Encodes an error reply frame describing <paramref name="exception"/> via the canonical mapping.</summary>
    public static byte[] Encode(Exception exception, DfsOperationId? op = null, string? leaderHint = null)
        => MessageCodec.Encode(ErrorReplyMessage.FromException(exception, op, leaderHint));

    /// <summary>Decodes an error reply frame.</summary>
    /// <exception cref="DfsProtocolException">The frame is malformed or is not an error reply.</exception>
    public static (DfsErrorCode Code, string Message, DfsOperationId? Op, bool Retryable) Decode(ReadOnlySpan<byte> data)
    {
        ErrorReplyMessage reply = MessageCodec.Decode<ErrorReplyMessage>(data);
        return (reply.Code, reply.Message, reply.OperationId, reply.Retryable);
    }

    /// <summary>Decodes an error reply frame and recreates its canonical exception.</summary>
    /// <exception cref="DfsProtocolException">The frame is malformed or is not an error reply.</exception>
    public static Exception DecodeToException(ReadOnlySpan<byte> data, string? path = null)
        => MessageCodec.Decode<ErrorReplyMessage>(data).ToException(path);
}
