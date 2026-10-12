namespace Tedd.Tdfs;

/// <summary>Membership status of a live node.</summary>
public enum DfsNodeStatus
{
    /// <summary>Serving normally.</summary>
    Active,

    /// <summary>Data is being moved off the node.</summary>
    Draining,

    /// <summary>Holds no required data; can be removed.</summary>
    Drained,

    /// <summary>Removed from the cluster.</summary>
    Removed,

    /// <summary>Member but currently unreachable.</summary>
    Unreachable,
}
