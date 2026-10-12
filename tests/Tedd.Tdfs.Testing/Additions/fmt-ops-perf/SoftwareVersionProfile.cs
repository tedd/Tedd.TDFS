namespace Tedd.Tdfs.Testing;

/// <summary>
/// A software release that the harness can run a live node (<see cref="TestNode.RestartAsSoftwareVersionAsync"/>) or a
/// backup node (<see cref="TestBackupNode.RestartAsSoftwareVersionAsync"/>) as. Used by the FMT suite to exercise disk
/// images written by every supported release, rolling upgrades, downgrade refusal and feature activation
/// (FMT-005..FMT-010; docs/api-additions/fmt-ops-perf.md).
/// </summary>
/// <remarks>
/// <para>
/// A profile fixes everything a release exposes at a compatibility boundary: the protocol version range it negotiates in
/// the handshake, the disk-format version it writes, the highest disk-format version it can read, and the cluster
/// feature flags it understands. A node running a profile behaves exactly as that release would: it advertises only
/// <see cref="Features"/>, never activates or serves state that requires a feature outside that set, writes
/// <see cref="DiskFormatVersion"/> for new structures, and refuses (see below) any image it cannot read.
/// </para>
/// <para>
/// Refusal contract (shared by every profile): when a node is started as a profile that cannot interpret its durable
/// image — the image's <c>MinimumReaderVersion</c> exceeds <see cref="MaxReadableDiskFormatVersion"/>, or the image
/// records an activated irreversible feature that is not in <see cref="Features"/> — the start fails with a
/// <see cref="DfsException"/> whose <see cref="DfsException.ErrorCode"/> is <see cref="DfsErrorCode.Unsupported"/>,
/// before anything is written to the medium (the durable image is byte-for-byte unchanged).
/// </para>
/// </remarks>
/// <param name="Name">Release name: "Current", "Previous" or "BelowSupportedWindow" for the built-in profiles.</param>
/// <param name="ProtocolVersionMin">Lowest protocol version this release negotiates.</param>
/// <param name="ProtocolVersionMax">Highest protocol version this release negotiates.</param>
/// <param name="DiskFormatVersion">Disk-format version this release writes for new superblocks/journals/checkpoints/segments.</param>
/// <param name="MaxReadableDiskFormatVersion">Highest disk-format version (and minimum-reader marker) this release can read.</param>
/// <param name="Features">Cluster feature flags this release understands and advertises.</param>
/// <param name="IrreversibleFeatures">
/// Subset of <paramref name="Features"/> whose activation is irreversible: once active, every live node durably records
/// a minimum-reader marker that releases lacking the feature cannot read, and <c>SetFeatureAsync(feature, false)</c>
/// is refused.
/// </param>
/// <param name="IsEmulated">
/// True when the profile is an in-process emulation of a release (for example <see cref="Previous"/> while the supported
/// window contains a single release) rather than a recorded older build.
/// </param>
public sealed record SoftwareVersionProfile(
    string Name,
    int ProtocolVersionMin,
    int ProtocolVersionMax,
    int DiskFormatVersion,
    int MaxReadableDiskFormatVersion,
    IReadOnlySet<string> Features,
    IReadOnlySet<string> IrreversibleFeatures,
    bool IsEmulated)
{
    /// <summary>
    /// Test-only reversible feature flag understood by <see cref="Current"/> and not by <see cref="Previous"/>. Activating
    /// it has no data effect; it only appears in <see cref="DfsClusterStatus.ActiveFeatures"/> and in
    /// <see cref="DfsServerCapabilities.Features"/> negotiated by new clients, and it may only become active while every
    /// live member (voter or learner) runs a release that understands it (FMT-009).
    /// </summary>
    public const string ProbeFeature = "tdfs.test.probe";

    /// <summary>
    /// Test-only irreversible feature flag understood by <see cref="Current"/> and not by <see cref="Previous"/>. Its
    /// activation follows the same eligibility rule as <see cref="ProbeFeature"/>; once active, every live node durably
    /// records a minimum-reader marker greater than <see cref="Previous"/>'s <see cref="MaxReadableDiskFormatVersion"/>
    /// (reported by <c>NodeInspector.GetDiskFormatAsync</c>), and the feature can never be deactivated (FMT-006).
    /// </summary>
    public const string IrreversibleProbeFeature = "tdfs.test.irreversible-probe";

    /// <summary>
    /// The build under test: protocol range <c>ProtocolInfo.MinSupportedVersion..ProtocolInfo.CurrentVersion</c>, its own
    /// disk format, and a feature set that includes <see cref="ProbeFeature"/> and <see cref="IrreversibleProbeFeature"/>.
    /// Nodes started by <see cref="TestCluster.StartAsync"/> run this profile.
    /// </summary>
    public static SoftwareVersionProfile Current => throw Stub.NotYet("SoftwareVersionProfile.Current");

    /// <summary>
    /// The oldest release inside the supported compatibility window. When the window contains only the current release,
    /// this is an emulation of <see cref="Current"/> (<see cref="IsEmulated"/> = true) with the same protocol range and disk
    /// format whose <see cref="Features"/> lack <see cref="ProbeFeature"/> and <see cref="IrreversibleProbeFeature"/> and
    /// whose <see cref="MaxReadableDiskFormatVersion"/> is below the marker recorded by <see cref="IrreversibleProbeFeature"/>.
    /// </summary>
    public static SoftwareVersionProfile Previous => throw Stub.NotYet("SoftwareVersionProfile.Previous");

    /// <summary>
    /// A release older than the supported window: its <see cref="MaxReadableDiskFormatVersion"/> is lower than
    /// <see cref="Current"/>'s <see cref="DiskFormatVersion"/> (and its protocol range does not overlap the current one), so
    /// it must refuse every image written by <see cref="Current"/> (emulated when no such recorded build exists).
    /// </summary>
    public static SoftwareVersionProfile BelowSupportedWindow => throw Stub.NotYet("SoftwareVersionProfile.BelowSupportedWindow");

    /// <summary>Every supported release, oldest first; the last entry is <see cref="Current"/> and the first is <see cref="Previous"/>.</summary>
    public static IReadOnlyList<SoftwareVersionProfile> SupportedWindow => throw Stub.NotYet("SoftwareVersionProfile.SupportedWindow");

    /// <summary>Resolves a built-in profile by name ("Current", "Previous" or "BelowSupportedWindow"), so theory data can stay serialisable.</summary>
    /// <exception cref="ArgumentException">Unknown name.</exception>
    public static SoftwareVersionProfile Get(string name) => name switch
    {
        "Current" => Current,
        "Previous" => Previous,
        "BelowSupportedWindow" => BelowSupportedWindow,
        _ => throw new ArgumentException($"Unknown software version profile '{name}'.", nameof(name)),
    };

    /// <summary>True when this release understands <paramref name="feature"/>.</summary>
    public bool Supports(string feature) => Features.Contains(feature);

    /// <inheritdoc />
    public override string ToString() =>
        $"{Name}(protocol {ProtocolVersionMin}..{ProtocolVersionMax}, disk {DiskFormatVersion}/read<={MaxReadableDiskFormatVersion}{(IsEmulated ? ", emulated" : string.Empty)})";
}
