using System.Collections.Frozen;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs;

/// <summary>Capabilities and limits advertised by the cluster during the handshake.</summary>
public sealed record DfsServerCapabilities
{
    /// <summary>Negotiated protocol version.</summary>
    public int ProtocolVersion { get; init; } = ProtocolInfo.CurrentVersion;

    /// <summary>Feature flags active cluster-wide (ordinal names).</summary>
    public IReadOnlySet<string> Features { get; init; } = FrozenSet<string>.Empty;

    /// <summary>Maximum supported file length.</summary>
    public long MaxFileLength { get; init; } = long.MaxValue / 2;

    /// <summary>Maximum normalized path size in UTF-8 bytes.</summary>
    public int MaxPathBytes { get; init; } = DfsPath.MaxPathBytes;

    /// <summary>Maximum component size in UTF-8 bytes.</summary>
    public int MaxComponentBytes { get; init; } = DfsPath.MaxComponentBytes;

    /// <summary>Maximum bytes published by one commit.</summary>
    public long MaxPublishBytes { get; init; } = 64L << 30;

    /// <summary>Default inline threshold in bytes.</summary>
    public int InlineThreshold { get; init; } = 4096;

    /// <summary>Logical update unit (chunk) size in bytes.</summary>
    public int ChunkSize { get; init; } = 1 << 20;

    /// <summary>Maximum stored object size in bytes.</summary>
    public int MaxObjectSize { get; init; } = 4 << 20;

    /// <summary>Default transaction limits.</summary>
    public DfsVolumeLimits TransactionLimits { get; init; } = new();

    /// <summary>Handle lease duration.</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);
}
