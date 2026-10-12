namespace Tedd.Tdfs;

/// <summary>The peer belongs to a different cluster than expected.</summary>
public class DfsClusterIdentityException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.ClusterMismatch"/>.</summary>
    public DfsClusterIdentityException(string message, Exception? inner = null)
        : base(DfsErrorCode.ClusterMismatch, message, inner)
    {
    }
}
