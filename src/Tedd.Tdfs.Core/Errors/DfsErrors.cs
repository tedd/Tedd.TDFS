namespace Tedd.Tdfs;

/// <summary>
/// Canonical mapping between <see cref="DfsErrorCode"/> and exceptions, shared by SDK and server (FMT-011).
/// <see cref="ToCode"/> inverts <see cref="ToException(DfsErrorCode, string, string?, DfsOperationId?)"/> for every
/// code, so codes, operation ids and retryability survive a round trip even when a standard .NET exception type is
/// used (the details are carried in <see cref="Exception.Data"/>).
/// </summary>
/// <remarks>
/// Standard mappings: <c>FileNotFound</c> → <see cref="FileNotFoundException"/> (FileName = path),
/// <c>DirectoryNotFound</c> → <see cref="DirectoryNotFoundException"/>, <c>AccessDenied</c> →
/// <see cref="UnauthorizedAccessException"/>, <c>InvalidPath</c> → <see cref="ArgumentException"/> (ParamName "path"),
/// <c>InvalidArgument</c> → <see cref="ArgumentException"/>, <c>Unsupported</c> → <see cref="NotSupportedException"/>,
/// <c>Cancelled</c> → <see cref="OperationCanceledException"/>, <c>Timeout</c> → <see cref="TimeoutException"/>.
/// Every other code maps to <see cref="DfsException"/> or one of its typed subclasses. For <c>TransactionConflict</c>
/// and <c>TransactionAborted</c> the supplied operation id (if any) also becomes the exception's transaction id;
/// <see cref="Protocol.Messages.ErrorReplyMessage.ToException"/> overrides it with the transported transaction id.
/// <c>None</c> and codes unknown to this version map to a plain <see cref="DfsException"/> carrying the exact code.
/// </remarks>
public static class DfsErrors
{
    /// <summary><see cref="Exception.Data"/> key holding the <see cref="DfsErrorCode"/> of a standard exception.</summary>
    public const string ErrorCodeDataKey = "Tedd.Tdfs.ErrorCode";

    /// <summary><see cref="Exception.Data"/> key holding the operation id (string form) of a standard exception.</summary>
    public const string OperationIdDataKey = "Tedd.Tdfs.OperationId";

    /// <summary><see cref="Exception.Data"/> key holding the volume path associated with the error.</summary>
    public const string PathDataKey = "Tedd.Tdfs.Path";

    /// <summary><see cref="Exception.Data"/> key holding the retryability flag of a standard exception.</summary>
    public const string RetryableDataKey = "Tedd.Tdfs.Retryable";

    /// <summary>Creates the canonical exception for <paramref name="code"/>.</summary>
    /// <param name="code">The error code.</param>
    /// <param name="message">The error message.</param>
    /// <param name="path">Volume path the error relates to (sets <see cref="FileNotFoundException.FileName"/>).</param>
    /// <param name="op">Operation id of the failed operation.</param>
    public static Exception ToException(DfsErrorCode code, string message, string? path = null, DfsOperationId? op = null)
        => Create(code, message, path, op, retryable: null);

    /// <summary>Creates the canonical exception for <paramref name="code"/> with explicit retryability (as decoded from the wire).</summary>
    public static Exception ToException(DfsErrorCode code, string message, string? path, DfsOperationId? op, bool retryable)
        => Create(code, message, path, op, retryable);

    /// <summary>Returns the canonical error code of an exception.</summary>
    public static DfsErrorCode ToCode(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        switch (ex)
        {
            case DfsException d:
                return d.ErrorCode;
            case DfsAuthenticationException a:
                return a.ErrorCode;
            case DfsRetentionProtectedException r:
                return r.ErrorCode;
            case DfsBackupNotReachedException b:
                return b.ErrorCode;
        }

        if (ex.Data[ErrorCodeDataKey] is DfsErrorCode tagged)
            return tagged;

        return ex switch
        {
            FileNotFoundException => DfsErrorCode.FileNotFound,
            DirectoryNotFoundException => DfsErrorCode.DirectoryNotFound,
            UnauthorizedAccessException => DfsErrorCode.AccessDenied,
            ArgumentException ae => string.Equals(ae.ParamName, "path", StringComparison.Ordinal) ? DfsErrorCode.InvalidPath : DfsErrorCode.InvalidArgument,
            NotSupportedException => DfsErrorCode.Unsupported,
            OperationCanceledException => DfsErrorCode.Cancelled,
            TimeoutException => DfsErrorCode.Timeout,
            AggregateException { InnerExceptions.Count: 1 } agg => ToCode(agg.InnerExceptions[0]),
            _ => DfsErrorCode.Internal,
        };
    }

    /// <summary>Returns whether retrying the same operation may succeed.</summary>
    public static bool IsRetryable(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        if (ex is DfsException d)
            return d.IsRetryable;
        if (ex.Data[RetryableDataKey] is bool tagged)
            return tagged;
        return ex is AggregateException { InnerExceptions.Count: 1 } agg && IsRetryable(agg.InnerExceptions[0]);
    }

    /// <summary>Returns the operation id carried by an exception, if any.</summary>
    public static DfsOperationId? GetOperationId(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        if (ex is DfsException d)
            return d.OperationId;
        if (ex.Data[OperationIdDataKey] is string s && DfsOperationId.TryParse(s, out var id))
            return id;
        return null;
    }

    /// <summary>Returns the volume path carried by an exception, if any.</summary>
    public static string? GetPath(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        if (ex.Data[PathDataKey] is string p)
            return p;
        return ex is FileNotFoundException fnf ? fnf.FileName : null;
    }

    /// <summary>The retryability a freshly created exception for <paramref name="code"/> reports by default.</summary>
    public static bool IsRetryableByDefault(DfsErrorCode code) => code switch
    {
        DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict or DfsErrorCode.TransactionConflict or
        DfsErrorCode.TransactionAborted or DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable or
        DfsErrorCode.PlacementUnavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.CapacityExhausted or
        DfsErrorCode.Unavailable => true,
        _ => false,
    };

    private static Exception Create(DfsErrorCode code, string message, string? path, DfsOperationId? op, bool? retryable)
    {
        message ??= string.Empty;
        Exception ex = code switch
        {
            DfsErrorCode.InvalidArgument => new ArgumentException(message),
            DfsErrorCode.InvalidPath => new ArgumentException(message, "path"),
            DfsErrorCode.FileNotFound => new FileNotFoundException(message, path),
            DfsErrorCode.DirectoryNotFound => new DirectoryNotFoundException(message),
            DfsErrorCode.AlreadyExists => new DfsAlreadyExistsException(message),
            DfsErrorCode.DirectoryNotEmpty => new DfsDirectoryNotEmptyException(message),
            DfsErrorCode.AccessDenied => new UnauthorizedAccessException(message),
            DfsErrorCode.AuthenticationFailed => new DfsAuthenticationException(message),
            DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict => new DfsSharingViolationException(code, message),
            DfsErrorCode.TransactionConflict => new DfsTransactionConflictException(TxOf(op), message),
            DfsErrorCode.TransactionAborted => new DfsTransactionAbortedException(TxOf(op), message),
            DfsErrorCode.LeaseLost => new DfsLeaseLostException(message),
            DfsErrorCode.PinExpired or DfsErrorCode.ContinuationExpired => new DfsPinExpiredException(code, message),
            DfsErrorCode.NotLeader or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable => new DfsUnavailableException(code, message),
            DfsErrorCode.QuorumUnavailable => new DfsQuorumUnavailableException(message),
            DfsErrorCode.PlacementUnavailable => new DfsPlacementUnavailableException(message),
            DfsErrorCode.CapacityExhausted => new DfsCapacityException(message),
            DfsErrorCode.Integrity => new DfsIntegrityException(message),
            DfsErrorCode.OutcomeUnknown => new DfsCommitOutcomeUnknownException(op.GetValueOrDefault(), message),
            DfsErrorCode.OperationExpired => new DfsOperationExpiredException(message),
            DfsErrorCode.OperationIdMismatch => new DfsOperationIdMismatchException(message),
            DfsErrorCode.LimitExceeded => new DfsLimitExceededException(message),
            DfsErrorCode.CrossVolume => new DfsCrossVolumeException(message),
            DfsErrorCode.ReadOnly => new DfsReadOnlyException(message),
            DfsErrorCode.Unsupported => new NotSupportedException(message),
            DfsErrorCode.ProtocolError => new DfsProtocolException(message),
            DfsErrorCode.ClusterMismatch => new DfsClusterIdentityException(message),
            DfsErrorCode.EpochMismatch => new DfsEpochMismatchException(message),
            DfsErrorCode.Timeout => new TimeoutException(message),
            DfsErrorCode.Cancelled => new OperationCanceledException(message),
            DfsErrorCode.RecoveryKeyMissing => new DfsRecoveryKeyMissingException(message),
            DfsErrorCode.AdapterFailed => new DfsSnapshotAdapterException(message),
            DfsErrorCode.RetentionProtected => new DfsRetentionProtectedException(message),
            DfsErrorCode.BackupNotReached => new DfsBackupNotReachedException(message),
            // None, Internal, NotADirectory, IsADirectory, SnapshotNotFound, BackupTargetNotFound, StaleIncarnation,
            // InvalidPolicy, InvalidHandle, VolumeNotFound and codes unknown to this version keep their exact code.
            _ => new DfsException(code, message),
        };

        if (path is not null)
            ex.Data[PathDataKey] = path;

        if (ex is DfsException dfs)
        {
            if (op.HasValue)
                dfs.SetOperationId(op);
            if (retryable.HasValue)
                dfs.SetRetryable(retryable.Value);
        }
        else
        {
            // Standard exception types cannot carry DFS properties; tag them so ToCode/GetOperationId round-trip exactly.
            ex.Data[ErrorCodeDataKey] = code;
            if (op.HasValue)
                ex.Data[OperationIdDataKey] = op.Value.ToString();
            ex.Data[RetryableDataKey] = retryable ?? IsRetryableByDefault(code);
        }

        return ex;
    }

    private static DfsTransactionId TxOf(DfsOperationId? op) => op.HasValue ? new DfsTransactionId(op.Value) : default;
}
