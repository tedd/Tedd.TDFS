namespace Tedd.Tdfs;

/// <summary>How metadata (Raft) voters of a volume group are placed.</summary>
public enum DfsVoterPlacement
{
    /// <summary>One voter per zone where possible.</summary>
    OnePerZone,
    /// <summary>Voters are the nodes named in <see cref="DfsMetadataPolicy.ExplicitVoterNodes"/>.</summary>
    Explicit,
    /// <summary>Any eligible nodes.</summary>
    Any,
}
