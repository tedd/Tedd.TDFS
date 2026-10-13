using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// The software release a node runs as: the protocol range it negotiates, the cluster features it understands (and
/// advertises in handshakes and to the control group) and the highest minimum-reader marker / format version of a durable
/// image it can open. Production nodes run <see cref="Current"/>; the test harness emulates other releases of the
/// compatibility window through <see cref="NodeEnvironment"/> (FMT-005..FMT-010, CHA-011).
/// </summary>
/// <param name="Name">Release name (diagnostics).</param>
/// <param name="ProtocolMin">Lowest protocol version negotiated.</param>
/// <param name="ProtocolMax">Highest protocol version negotiated.</param>
/// <param name="Features">Cluster features understood (ordinal set).</param>
/// <param name="MaxReadableFormat">Highest format version and minimum-reader marker of an image this release opens.</param>
internal sealed record NodeRelease(string Name, int ProtocolMin, int ProtocolMax, IReadOnlySet<string> Features, int MaxReadableFormat)
{
    /// <summary>This build.</summary>
    public static NodeRelease Current { get; } = new("Current", ProtocolInfo.MinSupportedVersion, ProtocolInfo.CurrentVersion, ProtocolFeatures.Supported,
        Superblock.SoftwareVersion);

    /// <summary>True when this release understands <paramref name="feature"/>.</summary>
    public bool Supports(string feature) => Features.Contains(feature);
}
