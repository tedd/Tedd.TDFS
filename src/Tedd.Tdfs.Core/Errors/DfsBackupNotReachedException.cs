namespace Tedd.Tdfs;

/// <summary>
/// An <c>AwaitBackup*</c> barrier timed out before the target published the required boundary. The live commit is
/// unaffected.
/// </summary>
public class DfsBackupNotReachedException : TimeoutException
{
    /// <summary>Creates the exception without target details.</summary>
    public DfsBackupNotReachedException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }

    /// <summary>Creates the exception for a target and its latest published checkpoint.</summary>
    public DfsBackupNotReachedException(string target, DfsBackupCheckpoint? lastPublished, string message, Exception? inner = null)
        : base(message, inner)
    {
        Target = target ?? string.Empty;
        LastPublished = lastPublished;
    }

    /// <summary>Always <see cref="DfsErrorCode.BackupNotReached"/>.</summary>
    public DfsErrorCode ErrorCode => DfsErrorCode.BackupNotReached;

    /// <summary>The backup target name (empty when unknown).</summary>
    public string Target { get; init; } = string.Empty;

    /// <summary>The latest checkpoint the target had published, if any.</summary>
    public DfsBackupCheckpoint? LastPublished { get; init; }
}
