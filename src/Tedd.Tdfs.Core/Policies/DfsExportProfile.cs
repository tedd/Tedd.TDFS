namespace Tedd.Tdfs;

/// <summary>Replicated volume policy for live subtree exports.</summary>
public enum DfsExportProfile
{
    Disabled,
    CaseSensitive,
    Windows,
}

/// <summary>A live export mapping, bound to its cluster, volume incarnation and directory.</summary>
public sealed record DfsExportRoot(CommitToken Volume, InodeId Directory)
{
    /// <summary>Cluster format capability required before persisting export commands, mappings or metadata.</summary>
    public const string ClusterFeature = "tdfs.live-exports.v1";
}

/// <summary>Independent desired access for an export reservation. None opens metadata only.</summary>
[Flags]
public enum DfsOpenAccess
{
    None = 0,
    Read = 1,
    Write = 2,
    Delete = 4,
}

/// <summary>Basic information persisted with the inode. Null fields preserve the current value on update.</summary>
public sealed record DfsBasicInformation(DateTimeOffset? CreatedUtc = null, DateTimeOffset? ModifiedUtc = null,
    DateTimeOffset? AccessedUtc = null, DateTimeOffset? ChangedUtc = null, FileAttributes? Attributes = null);

/// <summary>The effect of the committed open.</summary>
public enum DfsCreateResult { Opened, Created, Truncated }
