using System.Collections.Frozen;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Feature names this build understands (PROTOCOL.md §12). A feature becomes active cluster-wide only when every member
/// supports it (<c>AdminOp.SetFeature</c>, FMT-009).
/// </summary>
internal static class ProtocolFeatures
{
    /// <summary>Test-only reversible feature (FMT suite, <c>SoftwareVersionProfile.ProbeFeature</c>).</summary>
    public const string Probe = "tdfs.test.probe";

    /// <summary>Test-only irreversible feature (FMT suite, <c>SoftwareVersionProfile.IrreversibleProbeFeature</c>).</summary>
    public const string IrreversibleProbe = "tdfs.test.irreversible-probe";

    /// <summary>
    /// Test-only irreversible feature of the next (emulated) release (CHA-011, <c>SoftwareVersionProfile.NextFeature</c>).
    /// This build does not support it; it is listed here only so that every release applies the control log identically
    /// (irreversibility is part of the replicated decision).
    /// </summary>
    public const string NextFormat = "test.next-format";

    /// <summary>Every feature this build supports (ordinal set).</summary>
    public static IReadOnlySet<string> Supported { get; } = new[] { Probe, IrreversibleProbe }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Features that can never be deactivated once active (every known one, supported or not).</summary>
    public static IReadOnlySet<string> Irreversible { get; } = new[] { IrreversibleProbe, NextFormat }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// The minimum-reader marker (<see cref="Superblock.MinSoftwareVersion"/>) every node durably records once the irreversible
    /// <paramref name="feature"/> is active, so releases lacking the feature refuse the image (FMT-006); 0 for features that
    /// do not change the format.
    /// </summary>
    public static int ReaderMarkerOf(string feature) => feature switch
    {
        IrreversibleProbe => Superblock.BaseSoftwareVersion + 1,
        NextFormat => Superblock.BaseSoftwareVersion + 2,
        _ => 0,
    };
}
