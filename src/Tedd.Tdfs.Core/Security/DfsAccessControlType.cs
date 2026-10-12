namespace Tedd.Tdfs;

/// <summary>Whether an access control entry allows or denies its rights.</summary>
public enum DfsAccessControlType
{
    /// <summary>The entry grants its rights.</summary>
    Allow = 0,
    /// <summary>The entry denies its rights.</summary>
    Deny = 1,
}
