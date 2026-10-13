using System.Buffers;
using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Server.Volumes.Commands;

/// <summary>One name lookup a transaction depended on: <paramref name="Child"/> under <paramref name="Name"/> (0 = absent).</summary>
/// <param name="Directory">Directory inode the name was looked up in.</param>
/// <param name="Name">Entry name.</param>
/// <param name="Child">Inode the name resolved to at the read version, or 0 when absent.</param>
internal readonly record struct TxLookup(ulong Directory, string Name, ulong Child);

/// <summary>One inode version a transaction depended on (its content, metadata or entries).</summary>
/// <param name="Inode">Inode id.</param>
/// <param name="Revision">The inode's <see cref="State.InodeRecord.Revision"/> at the read version.</param>
internal readonly record struct TxRead(ulong Inode, ulong Revision);

/// <summary>One write of a transaction, in execution order.</summary>
/// <param name="Command">The encoded volume command as it executed on the transaction's working view.</param>
/// <param name="WorkingHandle">For an open: the handle id it created in the working view (later writes refer to it); 0 otherwise.</param>
internal readonly record struct TxWrite(ReadOnlyMemory<byte> Command, ulong WorkingHandle);

/// <summary>
/// The descriptor a <c>TxCommit</c> entry carries (ARCHITECTURE §5.6): the read set — name lookups and inode versions the
/// transaction observed at its read version — and the ordered writes. Apply validates every read item against the state
/// at the commit index (any change is a <c>TransactionConflict</c>) and then re-executes the writes atomically at that
/// index, so the commit index is the transaction's serialization point.
/// </summary>
/// <param name="Lookups">Name lookups (directory, name → child or absent).</param>
/// <param name="Reads">Inode versions.</param>
/// <param name="Writes">Ordered writes.</param>
internal sealed record TxDescriptor(ImmutableArray<TxLookup> Lookups, ImmutableArray<TxRead> Reads, ImmutableArray<TxWrite> Writes)
{
    /// <summary>Descriptor format version.</summary>
    public const byte Format = 1;

    /// <summary>The empty descriptor (no reads, no writes).</summary>
    public static TxDescriptor Empty { get; } = new([], [], []);

    /// <summary>Canonical encoding.</summary>
    public byte[] Encode()
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        var w = new WireWriter(buffer);
        w.WriteByte(Format);
        w.WriteCount(Lookups.Length);
        foreach (TxLookup l in Lookups)
        {
            w.WriteVarUInt(l.Directory);
            w.WriteString(l.Name);
            w.WriteVarUInt(l.Child);
        }

        w.WriteCount(Reads.Length);
        foreach (TxRead r in Reads)
        {
            w.WriteVarUInt(r.Inode);
            w.WriteVarUInt(r.Revision);
        }

        w.WriteCount(Writes.Length);
        foreach (TxWrite write in Writes)
        {
            w.WriteBytes(write.Command.Span);
            w.WriteVarUInt(write.WorkingHandle);
        }

        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes a descriptor (an empty input is the empty descriptor).</summary>
    /// <exception cref="DfsProtocolException">Malformed descriptor.</exception>
    public static TxDescriptor Decode(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return Empty;
        var r = new WireReader(data, new WireLimits(MaxBytesLength: int.MaxValue, MaxCollectionCount: 1 << 24));
        byte format = r.ReadByte();
        if (format != Format)
            throw WireReader.Malformed($"Unsupported transaction descriptor format {format}.");
        int lookupCount = r.ReadCount(3);
        var lookups = ImmutableArray.CreateBuilder<TxLookup>(lookupCount);
        for (int i = 0; i < lookupCount; i++)
            lookups.Add(new TxLookup(r.ReadVarUInt(), r.ReadString(), r.ReadVarUInt()));
        int readCount = r.ReadCount(2);
        var reads = ImmutableArray.CreateBuilder<TxRead>(readCount);
        for (int i = 0; i < readCount; i++)
            reads.Add(new TxRead(r.ReadVarUInt(), r.ReadVarUInt()));
        int writeCount = r.ReadCount(2);
        var writes = ImmutableArray.CreateBuilder<TxWrite>(writeCount);
        for (int i = 0; i < writeCount; i++)
        {
            byte[] command = r.ReadBytes().ToArray();
            writes.Add(new TxWrite(command, r.ReadVarUInt()));
        }

        r.EnsureEnd();
        return new TxDescriptor(lookups.MoveToImmutable(), reads.MoveToImmutable(), writes.MoveToImmutable());
    }
}
