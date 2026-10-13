using System.Buffers;
using System.Collections.Frozen;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Control;

// Per-pool placement epochs (Addendum A, pool.md: "Placement is stable for pools whose membership did not change").
//
// Deterministic targets are SelectTargets(object, epoch, policy, members) with the epoch created by the pool's last change
// of its placement-relevant membership (members, their eligibility and labels). A change of one pool advances the global
// placement epoch and records it as that pool's epoch only, so other pools keep their targets (POOL-014). Stored in the
// checkpoint's terminal extension block (optional field 1), so a checkpoint without pool epochs stays readable: all pools
// then use the global epoch until the next change initializes every pool's entry.
internal sealed partial class ControlState
{
    /// <summary>Extension field id of the pool epochs in the control checkpoint.</summary>
    private const uint PoolEpochsExtension = 1;

    private static readonly FrozenSet<uint> UnderstoodExtensions = FrozenSet.ToFrozenSet([PoolEpochsExtension]);

    /// <summary>Placement epoch of each pool's last placement-relevant change (<c>default</c> included).</summary>
    public SortedDictionary<string, ulong> PoolEpochs { get; } = new(StringComparer.Ordinal);

    /// <summary>The placement epoch deterministic targets in <paramref name="pool"/> are computed with.</summary>
    public ulong PoolEpoch(string pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        return PoolEpochs.TryGetValue(pool, out ulong epoch) ? epoch : PlacementEpoch;
    }

    /// <summary>The pools whose membership includes <paramref name="node"/>: <c>default</c> and every named pool listing it.</summary>
    public IEnumerable<string> PoolsContaining(NodeId node)
    {
        yield return DfsPoolNames.Default;
        foreach (PoolRecord pool in Pools.Values)
        {
            if (pool.Nodes.Contains(node))
                yield return pool.Name;
        }
    }

    /// <summary>Advances the global placement epoch and makes it the epoch of <paramref name="pools"/>.</summary>
    public void BumpPlacement(IEnumerable<string> pools)
    {
        ArgumentNullException.ThrowIfNull(pools);
        string[] changed = [.. pools];
        if (PoolEpochs.Count == 0)
        {
            // A state restored from a checkpoint without pool epochs: pin every pool to the epoch its targets used so far.
            PoolEpochs[DfsPoolNames.Default] = PlacementEpoch;
            foreach (string name in Pools.Keys)
                PoolEpochs[name] = PlacementEpoch;
        }

        PlacementEpoch++;
        foreach (string pool in changed)
            PoolEpochs[pool] = PlacementEpoch;
    }

    private IReadOnlyList<WireExtension> WriteExtensions()
    {
        if (PoolEpochs.Count == 0)
            return [];
        var buffer = new ArrayBufferWriter<byte>();
        var w = new WireWriter(buffer);
        w.WriteCount(PoolEpochs.Count);
        foreach ((string pool, ulong epoch) in PoolEpochs)
        {
            w.WriteString(pool);
            w.WriteVarUInt(epoch);
        }

        return [new WireExtension(PoolEpochsExtension, Required: false, buffer.WrittenMemory.ToArray())];
    }

    private void ReadExtension(WireExtension extension)
    {
        if (extension.FieldId != PoolEpochsExtension)
            return;
        var r = new WireReader(extension.Value.Span, new WireLimits(MaxBytesLength: int.MaxValue));
        int count = r.ReadCount(2);
        for (int i = 0; i < count; i++)
        {
            string pool = r.ReadString();
            PoolEpochs[pool] = r.ReadVarUInt();
        }

        r.EnsureEnd();
    }
}
