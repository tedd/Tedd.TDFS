namespace Tedd.Tdfs;

/// <summary>Status of an operation as known by the volume (FSD §13).</summary>
public enum DfsOutcomeStatus
{
    /// <summary>The operation committed.</summary>
    Committed,

    /// <summary>The operation did not and will never take effect.</summary>
    Aborted,

    /// <summary>The operation is known and still in flight.</summary>
    Pending,

    /// <summary>The operation has not been seen (only reported when not resolving).</summary>
    NotFound,

    /// <summary>The operation id is outside the retained outcome window.</summary>
    Expired,
}
