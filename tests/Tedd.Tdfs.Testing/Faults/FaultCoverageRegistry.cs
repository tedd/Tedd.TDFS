namespace Tedd.Tdfs.Testing;

/// <summary>Records every fault-point hit so tests can prove intended points were reached.</summary>
public sealed class FaultCoverageRegistry
{
    internal FaultCoverageRegistry()
    {
    }

    /// <summary>All hits in order.</summary>
    public IReadOnlyList<FaultContext> Hits => throw Stub.NotYet("FaultCoverageRegistry.Hits");

    /// <summary>True when <paramref name="site"/> was hit (optionally on <paramref name="node"/>).</summary>
    public bool WasReached(string site, string? node = null) => throw Stub.NotYet("FaultCoverageRegistry.WasReached");

    /// <summary>Hit counts per site.</summary>
    public IReadOnlyDictionary<string, int> CountsBySite() => throw Stub.NotYet("FaultCoverageRegistry.CountsBySite");

    /// <summary>Builds a campaign report (test plan §8.4).</summary>
    public CampaignReport CreateReport(string campaign, int generated, int applicable, int excluded) =>
        throw Stub.NotYet("FaultCoverageRegistry.CreateReport");
}
