namespace Tedd.Tdfs;

/// <summary>Metadata (volume Raft group) placement policy.</summary>
public sealed record DfsMetadataPolicy
{
    /// <summary>Number of voters (default 3).</summary>
    public int Voters { get; init; } = 3;

    /// <summary>Voter placement strategy.</summary>
    public DfsVoterPlacement Placement { get; init; } = DfsVoterPlacement.OnePerZone;

    /// <summary>Node names of the voters, for <see cref="DfsVoterPlacement.Explicit"/>.</summary>
    public IReadOnlyList<string>? ExplicitVoterNodes { get; init; }

    /// <summary>Validates internal consistency.</summary>
    /// <exception cref="DfsException">With <see cref="DfsErrorCode.InvalidPolicy"/> when the policy is inconsistent.</exception>
    public void Validate()
    {
        string? error = GetValidationError();
        if (error is not null)
            throw new DfsException(DfsErrorCode.InvalidPolicy, error);
    }

    /// <summary>Returns the first violated rule, or null when the policy is valid.</summary>
    public string? GetValidationError()
    {
        if (Voters < 1)
            return $"Voters must be at least 1 (was {Voters}).";
        if (!Enum.IsDefined(Placement))
            return $"Unknown voter placement '{Placement}'.";
        if (Placement == DfsVoterPlacement.Explicit)
        {
            if (ExplicitVoterNodes is null || ExplicitVoterNodes.Count == 0)
                return "Explicit voter placement requires ExplicitVoterNodes.";
            if (ExplicitVoterNodes.Count != Voters)
                return $"ExplicitVoterNodes lists {ExplicitVoterNodes.Count} nodes but Voters is {Voters}.";
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (string? node in ExplicitVoterNodes)
            {
                if (string.IsNullOrEmpty(node))
                    return "ExplicitVoterNodes must not contain empty node names.";
                if (!seen.Add(node))
                    return $"ExplicitVoterNodes lists node '{node}' more than once.";
            }
        }

        return null;
    }
}
