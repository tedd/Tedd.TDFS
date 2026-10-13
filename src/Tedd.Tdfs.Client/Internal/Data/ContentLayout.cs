using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Client.Internal.Data;

/// <summary>
/// Representation rules the SDK applies before publishing (PROTOCOL.md §5.2, §8.1, FSD LAY-002): content is split into
/// objects deterministically at <see cref="DfsServerCapabilities.ChunkSize"/> boundaries of the file, and stored inline
/// only when the resulting stream fits the volume's inline threshold <b>and</b> the metadata group's durability
/// satisfies the file's storage policy (every majority of the volume's voters spans at least
/// <see cref="DfsStoragePolicy.DurableCopiesBeforeAck"/> hosts and <see cref="DfsStoragePolicy.DurableZonesBeforeAck"/>
/// zones). Otherwise the bytes are uploaded as objects meeting the policy before the publication is sent.
/// </summary>
internal static class ContentLayout
{
    private const int MaxExactVoters = 15;

    /// <summary>The chunk size used to split content (positive).</summary>
    public static int ChunkSize(DfsServerCapabilities capabilities)
    {
        int chunk = capabilities.ChunkSize > 0 ? capabilities.ChunkSize : 1 << 20;
        int max = capabilities.MaxObjectSize > 0 ? capabilities.MaxObjectSize : chunk;
        return Math.Min(chunk, max);
    }

    /// <summary>The volume's inline threshold.</summary>
    public static int InlineThreshold(VolumeEntry? volume, DfsServerCapabilities capabilities) =>
        volume?.InlineThreshold ?? capabilities.InlineThreshold;

    /// <summary>
    /// Resolves a policy name of the volume: null or empty → the default policy; otherwise the named policy, or null when
    /// the volume does not define it (the server then rejects the request with <c>InvalidPolicy</c>).
    /// </summary>
    public static DfsStoragePolicy? ResolvePolicy(VolumeEntry? volume, string? name)
    {
        if (volume is null)
            return null;
        if (string.IsNullOrEmpty(name) || string.Equals(name, "default", StringComparison.Ordinal))
            return volume.DefaultPolicy;
        return volume.NamedPolicies.TryGetValue(name, out DfsStoragePolicy? policy) ? policy : null;
    }

    /// <summary>The storage pool objects of <paramref name="policy"/> are placed in.</summary>
    public static string PoolOf(VolumeEntry? volume, DfsStoragePolicy policy) =>
        policy.Pool ?? volume?.StoragePool ?? DfsPoolNames.Default;

    /// <summary>
    /// True when the metadata group of <paramref name="volume"/> is durable enough to hold inline data of
    /// <paramref name="policy"/>: the weakest majority of its voters spans enough distinct hosts and zones. Unknown voters
    /// count as unable to contribute (conservative).
    /// </summary>
    public static bool InlinePermitted(VolumeEntry volume, DfsStoragePolicy policy, Func<NodeId, DfsTopologyLabels?> labelsOf)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(policy);
        int voters = volume.Voters.Count;
        if (voters == 0)
            return false;
        int majority = voters / 2 + 1;
        var labels = new DfsTopologyLabels?[voters];
        for (int i = 0; i < voters; i++)
            labels[i] = labelsOf(volume.Voters[i]);

        int requiredCopies = policy.DurableCopiesBeforeAck;
        int requiredZones = policy.DurableZonesBeforeAck;
        if (voters > MaxExactVoters)
        {
            // Too many subsets to enumerate: count the guarantee of the worst case (all unknown) conservatively.
            return false;
        }

        // Every majority must satisfy the policy; only minimal majorities (exactly `majority` members) matter.
        int limit = 1 << voters;
        for (int mask = 0; mask < limit; mask++)
        {
            if (System.Numerics.BitOperations.PopCount((uint)mask) != majority)
                continue;
            var hosts = new HashSet<string>(StringComparer.Ordinal);
            var zones = new HashSet<(string, string)>();
            for (int i = 0; i < voters; i++)
            {
                if ((mask & (1 << i)) == 0 || labels[i] is not { } l)
                    continue;
                hosts.Add(l.Host);
                zones.Add(l.ZoneKey);
            }

            if (hosts.Count < requiredCopies || zones.Count < requiredZones)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Splits <c>[start, end)</c> of a file into pieces at chunk boundaries (absolute file offsets), so equal content
    /// always yields the same object boundaries.
    /// </summary>
    public static IEnumerable<(long Offset, int Length)> Chunks(long start, long end, int chunkSize)
    {
        long position = start;
        while (position < end)
        {
            long boundary = (position / chunkSize + 1) * chunkSize;
            long pieceEnd = Math.Min(end, boundary);
            yield return (position, checked((int)(pieceEnd - position)));
            position = pieceEnd;
        }
    }

    /// <summary>A <see cref="RangeChange"/> of inline bytes.</summary>
    public static RangeChange InlineChange(long offset, ReadOnlyMemory<byte> bytes) =>
        new(offset, bytes.Length, RangeChangeKind.Inline, default, 0, 0, UInt128.Zero, ObjectEncodings.Plain, bytes);

    /// <summary>A <see cref="RangeChange"/> mapping <c>[offset, offset + length)</c> to a whole uploaded object.</summary>
    public static RangeChange DataChange(long offset, ObjectId obj, long length, UInt128 checksum) =>
        new(offset, length, RangeChangeKind.Data, obj, 0, length, checksum, ObjectEncodings.Plain, ReadOnlyMemory<byte>.Empty);

    /// <summary>A <see cref="RangeChange"/> turning <c>[offset, offset + length)</c> into a hole.</summary>
    public static RangeChange ZeroChange(long offset, long length) =>
        new(offset, length, RangeChangeKind.Zero, default, 0, 0, UInt128.Zero, ObjectEncodings.Plain, ReadOnlyMemory<byte>.Empty);
}
