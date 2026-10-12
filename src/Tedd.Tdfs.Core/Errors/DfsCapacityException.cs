namespace Tedd.Tdfs;

/// <summary>Admission rejected new work because storage capacity is exhausted (<see cref="DfsErrorCode.CapacityExhausted"/>).</summary>
public class DfsCapacityException : DfsUnavailableException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.CapacityExhausted"/>.</summary>
    public DfsCapacityException(string message, Exception? inner = null)
        : base(DfsErrorCode.CapacityExhausted, message, inner)
    {
    }
}
