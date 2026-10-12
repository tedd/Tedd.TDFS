using System.Buffers;
using System.Buffers.Binary;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>
/// Self-describing image of a group's state at one log position: the payload of local checkpoints and of Raft snapshots
/// sent to lagging followers. Layout: <c>u32 magic "TGSN" | u8 version | varuint index | varuint term |
/// list&lt;NodeId&gt; voters | list&lt;NodeId&gt; learners | u128 XxHash128(state) | bytes state</c>. A receiver verifies the
/// identity (index, term, membership) against the Raft metadata and the hash against the state before installing anything
/// (RSY-015: a corrupt or mismatched image is never activated).
/// </summary>
internal static class SnapshotEnvelope
{
    private const uint Magic = 0x4E534754; // "TGSN" little-endian
    private const byte Version = 1;

    /// <summary>Builds the envelope of <paramref name="state"/> at <paramref name="meta"/>.</summary>
    public static byte[] Encode(RaftSnapshotMeta meta, ReadOnlySpan<byte> state)
    {
        ArgumentNullException.ThrowIfNull(meta);
        var buffer = new ArrayBufferWriter<byte>(state.Length + 96);
        var writer = new WireWriter(buffer);
        writer.WriteUInt32(Magic);
        writer.WriteByte(Version);
        writer.WriteVarUInt(meta.Index);
        writer.WriteVarUInt(meta.Term);
        RaftConfCodec.Write(ref writer, meta.ConfState);
        writer.WriteUInt128(ObjectChecksum.Compute(state));
        writer.WriteBytes(state);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Decodes and verifies an envelope. False (with a reason) when it is malformed, its hash does not match, or it does
    /// not describe <paramref name="expected"/> (when given).
    /// </summary>
    public static bool TryDecode(ReadOnlyMemory<byte> envelope, RaftSnapshotMeta? expected, out RaftSnapshotMeta meta,
        out ReadOnlyMemory<byte> state, out string? error)
    {
        meta = RaftSnapshotMeta.Empty;
        state = ReadOnlyMemory<byte>.Empty;
        error = null;
        try
        {
            var reader = new WireReader(envelope.Span, new WireLimits(MaxBytesLength: int.MaxValue));
            if (envelope.Length < 5 || BinaryPrimitives.ReadUInt32LittleEndian(envelope.Span) != Magic)
            {
                error = "not a group snapshot image (bad magic)";
                return false;
            }

            reader.ReadUInt32();
            if (reader.ReadByte() != Version)
            {
                error = "unsupported group snapshot image version";
                return false;
            }

            ulong index = reader.ReadVarUInt();
            ulong term = reader.ReadVarUInt();
            RaftConfState conf = RaftConfCodec.Read(ref reader);
            UInt128 hash = reader.ReadUInt128();
            int stateLength = reader.ReadBytes().Length;
            reader.EnsureEnd();
            int stateOffset = envelope.Length - stateLength;
            state = envelope.Slice(stateOffset, stateLength);
            if (ObjectChecksum.Compute(state.Span) != hash)
            {
                error = "group snapshot image checksum mismatch";
                return false;
            }

            meta = new RaftSnapshotMeta(index, term, conf);
            if (expected is not null && (expected.Index != index || expected.Term != term || !expected.ConfState.Equals(conf)))
            {
                error = $"group snapshot image describes ({index}, {term}) but ({expected.Index}, {expected.Term}) was announced";
                return false;
            }

            return true;
        }
        catch (DfsProtocolException ex)
        {
            error = "malformed group snapshot image: " + ex.Message;
            return false;
        }
    }
}
