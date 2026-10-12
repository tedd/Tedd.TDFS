namespace Tedd.Tdfs;

/// <summary>Which revision reads on a stream observe.</summary>
public enum DfsReadView
{
    /// <summary>Each read call observes the latest committed revision (linearizable).</summary>
    Live,

    /// <summary>All reads observe the revision pinned at open (read-only handles outside transactions).</summary>
    VersionPinned,
}
