namespace Tedd.Tdfs;

/// <summary>The operation would address more than one volume (for example a move or token from another volume).</summary>
public class DfsCrossVolumeException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.CrossVolume"/>.</summary>
    public DfsCrossVolumeException(string message, Exception? inner = null)
        : base(DfsErrorCode.CrossVolume, message, inner)
    {
    }
}
