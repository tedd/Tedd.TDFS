namespace Tedd.Tdfs;

/// <summary>
/// Integrity verification failed and no verified copy was available (<see cref="DfsErrorCode.Integrity"/>).
/// Data is never substituted with zeroes or a fabricated end of file.
/// </summary>
public class DfsIntegrityException : DfsException
{
    /// <summary>Creates the exception without an object id.</summary>
    public DfsIntegrityException(string message, Exception? inner = null)
        : base(DfsErrorCode.Integrity, message, inner)
    {
    }

    /// <summary>Creates the exception for a specific object.</summary>
    public DfsIntegrityException(ObjectId objectId, string message, Exception? inner = null)
        : base(DfsErrorCode.Integrity, message, inner)
    {
        ObjectId = objectId;
    }

    /// <summary>The object that failed verification, when applicable.</summary>
    public ObjectId? ObjectId { get; init; }
}
