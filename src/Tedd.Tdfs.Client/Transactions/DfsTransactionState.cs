namespace Tedd.Tdfs;

/// <summary>Client-side state of a <see cref="DfsTransaction"/>.</summary>
public enum DfsTransactionState
{
    /// <summary>Accepting operations.</summary>
    Active,

    /// <summary>A commit request is in flight.</summary>
    Committing,

    /// <summary>Committed.</summary>
    Committed,

    /// <summary>Aborted (explicitly, by conflict, or by the server).</summary>
    Aborted,

    /// <summary>The commit outcome is unknown; resolve it by transaction id.</summary>
    Unknown,
}
