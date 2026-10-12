namespace Tedd.Tdfs;

/// <summary>Writer authority (lease or fencing generation) was lost; the stream is faulted and its publication rejected.</summary>
public class DfsLeaseLostException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.LeaseLost"/>.</summary>
    public DfsLeaseLostException(string message, Exception? inner = null)
        : base(DfsErrorCode.LeaseLost, message, inner)
    {
    }
}
