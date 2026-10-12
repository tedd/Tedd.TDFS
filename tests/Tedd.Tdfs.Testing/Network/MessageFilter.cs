using System.Buffers.Binary;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>Selects messages for network rules, holds and statistics. Null members match anything.</summary>
/// <param name="From">Sender name ("n0", "b0", "client:c0").</param>
/// <param name="To">Receiver name.</param>
/// <param name="Type">Exact message type.</param>
/// <param name="Operation">Logical operation (<see cref="MessageTypes.OperationOf"/> of the message type).</param>
/// <param name="Direction">Request, reply or one-way.</param>
/// <param name="Predicate">Predicate over the decoded message; frames that cannot be decoded do not match.</param>
public sealed record MessageFilter(string? From = null, string? To = null, MessageType? Type = null, DfsOperationKind? Operation = null,
    LinkDirection? Direction = null, Func<IWireMessage, bool>? Predicate = null)
{
    /// <summary>A filter matching every message.</summary>
    public static MessageFilter Any { get; } = new();

    /// <summary>
    /// Tests a message frame (<c>[u16 little-endian type][body]</c>) sent from <paramref name="from"/> to <paramref name="to"/>.
    /// The frame is only decoded when <see cref="Predicate"/> is set.
    /// </summary>
    public bool Matches(string from, string to, LinkDirection direction, ReadOnlySpan<byte> frame)
    {
        if (From is not null && !string.Equals(From, from, StringComparison.Ordinal))
            return false;
        if (To is not null && !string.Equals(To, to, StringComparison.Ordinal))
            return false;
        if (Direction is not null && Direction.Value != direction)
            return false;

        if (Type is not null || Operation is not null)
        {
            if (frame.Length < sizeof(ushort))
                return false;
            var type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(frame);
            if (Type is not null && Type.Value != type)
                return false;
            if (Operation is not null && MessageTypes.OperationOf(type) != Operation.Value)
                return false;
        }

        if (Predicate is not null)
        {
            IWireMessage message;
            try
            {
                message = MessageCodec.Decode(frame);
            }
            catch (Exception ex) when (ex is DfsException or ArgumentException or InvalidOperationException or FormatException)
            {
                return false;
            }

            return Predicate(message);
        }

        return true;
    }

    /// <summary>Tests already-extracted message metadata; a set <see cref="Predicate"/> is evaluated only when <paramref name="message"/> is given.</summary>
    public bool Matches(string from, string to, LinkDirection direction, MessageType type, IWireMessage? message)
    {
        if (From is not null && !string.Equals(From, from, StringComparison.Ordinal))
            return false;
        if (To is not null && !string.Equals(To, to, StringComparison.Ordinal))
            return false;
        if (Direction is not null && Direction.Value != direction)
            return false;
        if (Type is not null && Type.Value != type)
            return false;
        if (Operation is not null && MessageTypes.OperationOf(type) != Operation.Value)
            return false;
        if (Predicate is not null)
            return message is not null && Predicate(message);
        return true;
    }
}
