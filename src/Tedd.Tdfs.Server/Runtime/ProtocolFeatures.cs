using System.Collections.Frozen;

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

    /// <summary>Every feature this build supports (ordinal set).</summary>
    public static IReadOnlySet<string> Supported { get; } = new[] { Probe, IrreversibleProbe }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Features that can never be deactivated once active.</summary>
    public static IReadOnlySet<string> Irreversible { get; } = new[] { IrreversibleProbe }.ToFrozenSet(StringComparer.Ordinal);
}
