using System.Diagnostics.CodeAnalysis;

namespace Tedd.Tdfs.Consensus;

/// <summary>A membership change proposal (one at a time; effective when appended).</summary>
/// <param name="Kind">Change kind.</param>
/// <param name="Node">Affected node.</param>
public sealed record RaftConfChange(RaftConfChangeKind Kind, NodeId Node)
{
    /// <summary>Format version of the encoded form.</summary>
    public const byte FormatVersion = 1;

    /// <summary>Length in bytes of the encoded form: version, kind, 16-byte node id.</summary>
    public const int EncodedLength = 2 + 16;

    /// <summary>
    /// Encodes the change as the payload of a <see cref="RaftEntryKind.ConfChange"/> entry:
    /// <c>[u8 version=1][u8 kind][16 bytes node id]</c>.
    /// </summary>
    public byte[] Encode()
    {
        var bytes = new byte[EncodedLength];
        bytes[0] = FormatVersion;
        bytes[1] = (byte)Kind;
        if (!Node.Value.TryWriteBytes(bytes.AsSpan(2)))
            throw new InvalidOperationException("Failed to encode the node id.");
        return bytes;
    }

    /// <summary>Decodes a payload produced by <see cref="Encode"/>.</summary>
    /// <returns>False when the payload is malformed (wrong length, unknown version or kind).</returns>
    public static bool TryDecode(ReadOnlySpan<byte> data, [NotNullWhen(true)] out RaftConfChange? change)
    {
        change = null;
        if (data.Length != EncodedLength || data[0] != FormatVersion)
            return false;
        var kind = (RaftConfChangeKind)data[1];
        if (kind is not (RaftConfChangeKind.AddLearner or RaftConfChangeKind.PromoteLearner or RaftConfChangeKind.RemoveNode))
            return false;
        change = new RaftConfChange(kind, new NodeId(new Guid(data.Slice(2, 16))));
        return true;
    }

    /// <summary>Decodes a payload produced by <see cref="Encode"/>.</summary>
    /// <exception cref="DfsProtocolException">The payload is malformed.</exception>
    public static RaftConfChange Decode(ReadOnlySpan<byte> data)
        => TryDecode(data, out RaftConfChange? change)
            ? change
            : throw new DfsProtocolException("Malformed Raft configuration change payload.");
}
