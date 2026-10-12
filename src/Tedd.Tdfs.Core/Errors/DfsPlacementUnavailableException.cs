namespace Tedd.Tdfs;

/// <summary>
/// Durable data placement required by the storage policy could not be achieved
/// (<see cref="DfsErrorCode.PlacementUnavailable"/>). Distinct from quorum unavailability; nothing was published.
/// </summary>
public class DfsPlacementUnavailableException : DfsUnavailableException
{
    /// <summary>Creates the exception without domain counts.</summary>
    public DfsPlacementUnavailableException(string message, Exception? inner = null)
        : base(DfsErrorCode.PlacementUnavailable, message, inner)
    {
    }

    /// <summary>Creates the exception with the required and achieved durable copy/zone counts.</summary>
    public DfsPlacementUnavailableException(string message, int requiredCopies, int achievedCopies, int requiredZones, int achievedZones, Exception? inner = null)
        : base(DfsErrorCode.PlacementUnavailable, message, inner)
    {
        RequiredCopies = requiredCopies;
        AchievedCopies = achievedCopies;
        RequiredZones = requiredZones;
        AchievedZones = achievedZones;
    }

    /// <summary>Durable copies (distinct hosts) required before acknowledgement.</summary>
    public int RequiredCopies { get; init; }

    /// <summary>Durable copies (distinct hosts) achieved.</summary>
    public int AchievedCopies { get; init; }

    /// <summary>Distinct zones required before acknowledgement.</summary>
    public int RequiredZones { get; init; }

    /// <summary>Distinct zones achieved.</summary>
    public int AchievedZones { get; init; }
}
