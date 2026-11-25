namespace Tedd.Tdfs.Client.Internal;

/// <summary>
/// Local validation of SDK arguments and options (SDK-001, PATH-*, OPEN-019): everything is checked before a request is
/// sent, so invalid input never reaches the cluster and never mutates anything.
/// </summary>
internal static class Validation
{
    private const FileShare KnownShareBits = FileShare.ReadWrite | FileShare.Delete | FileShare.Inheritable;
    private const FileOptions SupportedOptions = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.RandomAccess;

    /// <summary>Validates and NFC-normalizes a path (<see cref="ArgumentNullException"/>/<see cref="ArgumentException"/> naming <paramref name="parameter"/>).</summary>
    public static string Path(string? path, string parameter = "path")
    {
        if (path is null)
            throw new ArgumentNullException(parameter);
        if (!DfsPath.TryNormalize(path, out string normalized, out string? error))
            throw DfsPath.InvalidPath($"Invalid path '{Printable(path)}': {error}", parameter);
        return normalized;
    }

    /// <summary>A directory path: stream syntax is invalid (ADS-06).</summary>
    public static string DirectoryPath(string? path, string parameter = "path")
    {
        string normalized = Path(path, parameter);
        if (DfsStreamPath.HasStream(normalized))
            throw DfsPath.InvalidPath($"Invalid directory path '{Printable(normalized)}': directories have no named streams.", parameter);
        return normalized;
    }

    /// <summary>A file path that may address a named stream but not the root.</summary>
    public static string FilePath(string? path, string parameter = "path")
    {
        string normalized = Path(path, parameter);
        if (DfsPath.IsRoot(normalized))
            throw DfsPath.InvalidPath("The root is a directory, not a file.", parameter);
        return normalized;
    }

    /// <summary>A path printable in messages (control characters escaped, bounded length).</summary>
    public static string Printable(string path)
    {
        var chars = new System.Text.StringBuilder(Math.Min(path.Length, 300));
        foreach (char c in path)
        {
            if (chars.Length >= 300)
            {
                chars.Append('…');
                break;
            }

            if (char.IsControl(c))
                chars.Append("\\u").Append(((int)c).ToString("x4", System.Globalization.CultureInfo.InvariantCulture));
            else
                chars.Append(c);
        }

        return chars.ToString();
    }

    /// <summary>Validates mutation options (null → <see cref="ArgumentNullException"/>).</summary>
    public static void MutationOptions(DfsMutationOptions? options, string parameter = "options")
    {
        if (options is null)
            throw new ArgumentNullException(parameter);
        if (options.OperationId is { IsEmpty: true })
            throw new ArgumentException("OperationId must not be the empty id; obtain one with DfsVolume.NewOperationIdAsync.", parameter);
        if (options.StoragePolicy is { Length: 0 })
            throw new ArgumentException("StoragePolicy must not be empty (use null for the default policy).", parameter);
        if (options.CommitSnapshotName is { Length: 0 })
            throw new ArgumentException("CommitSnapshotName must not be empty (use null for no snapshot).", parameter);
    }

    /// <summary>Validates open options (FSD §6.1, OPEN-019); <paramref name="transactional"/> forbids version-pinned views.</summary>
    public static void OpenOptions(DfsOpenOptions? options, bool transactional, string parameter = "options")
    {
        if (options is null)
            throw new ArgumentNullException(parameter);
        if (!Enum.IsDefined(options.Mode))
            throw new ArgumentException($"Unknown FileMode value {(int)options.Mode}.", parameter);
        if (!(options.ExportRoot is not null && options.Access == 0) && options.Access is not (FileAccess.Read or FileAccess.Write or FileAccess.ReadWrite))
            throw new ArgumentException($"Unknown FileAccess value {(int)options.Access}.", parameter);
        if ((options.Share & ~KnownShareBits) != 0)
            throw new ArgumentException($"Unknown FileShare bits 0x{(int)(options.Share & ~KnownShareBits):x}.", parameter);
        if ((options.Share & FileShare.Inheritable) != 0)
            throw new NotSupportedException("FileShare.Inheritable is not supported: DfsStream handles are not operating-system handles.");
        if (!Enum.IsDefined(options.WriteMode))
            throw new ArgumentException($"Unknown DfsWriteMode value {(int)options.WriteMode}.", parameter);
        if (!Enum.IsDefined(options.ReadView))
            throw new ArgumentException($"Unknown DfsReadView value {(int)options.ReadView}.", parameter);
        if (options.BufferSize <= 0)
            throw new ArgumentOutOfRangeException(parameter, options.BufferSize, "BufferSize must be positive.");
        if ((options.Options & ~SupportedOptions) != 0)
        {
            throw new NotSupportedException(
                $"FileOptions {options.Options} are not supported (supported: None, Asynchronous, SequentialScan, RandomAccess).");
        }

        bool writable = (options.Access & FileAccess.Write) != 0;
        switch (options.Mode)
        {
            case FileMode.CreateNew or FileMode.Create when !writable && !options.Directory && !(options.ExportRoot is not null && options.Mode == FileMode.CreateNew):
                throw new ArgumentException($"FileMode.{options.Mode} requires write access.", parameter);
            case FileMode.Truncate when !writable || (options.ExportRoot is null && options.Access != FileAccess.Write):
            case FileMode.Append when options.Access != FileAccess.Write:
                throw new ArgumentException($"FileMode.{options.Mode} requires FileAccess.Write (and no read access).", parameter);
        }

        if (options.ExportRoot is null && writable && (options.Share & FileShare.Write) != 0)
            throw new ArgumentException("A writable handle cannot grant FileShare.Write (single writer per file).", parameter);
        if (options.ReadView == DfsReadView.VersionPinned && writable)
            throw new ArgumentException("A version-pinned view is read-only; open writable handles with DfsReadView.Live.", parameter);
        if (options.ReadView == DfsReadView.VersionPinned && transactional)
            throw new ArgumentException("Transaction-bound handles read the transaction's view; DfsReadView.VersionPinned is not available inside a transaction.", parameter);
        if (options.StoragePolicy is { Length: 0 })
            throw new ArgumentException("StoragePolicy must not be empty (use null for the default policy).", parameter);
        if (options.OperationId is { IsEmpty: true })
            throw new ArgumentException("OperationId must not be the empty id; obtain one with DfsVolume.NewOperationIdAsync.", parameter);
    }

    /// <summary>Validates enumeration options (null → defaults).</summary>
    public static DfsEnumerationOptions EnumerationOptions(DfsEnumerationOptions? options, string parameter = "options")
    {
        options ??= new DfsEnumerationOptions();
        if (options.PageSize is < 1 or > 10_000)
            throw new ArgumentOutOfRangeException(parameter, options.PageSize, "PageSize must be between 1 and 10000.");
        if (options.Kinds == 0 || (options.Kinds & ~DfsEntryKinds.All) != 0)
            throw new ArgumentException($"Invalid entry kinds {(int)options.Kinds}.", parameter);
        if (options.Pattern is null)
            throw new ArgumentException("Pattern must not be null (use \"*\" for every entry).", parameter);
        return options;
    }

    /// <summary>A non-empty name (snapshot, target, policy, group, pool, principal, node).</summary>
    public static string Name(string? name, string parameter)
    {
        if (name is null)
            throw new ArgumentNullException(parameter);
        if (name.Length == 0)
            throw new ArgumentException("The name must not be empty.", parameter);
        return name;
    }

    /// <summary>A strictly positive timeout.</summary>
    public static TimeSpan PositiveTimeout(TimeSpan timeout, string parameter)
    {
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(parameter, timeout, "The timeout must be positive.");
        return timeout;
    }

    /// <summary>Validates backup budgets.</summary>
    public static void Budgets(DfsBackupBudgets? budgets, string parameter = "budgets")
    {
        if (budgets is null)
            throw new ArgumentNullException(parameter);
        if (budgets.MaxPinAge <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(parameter, budgets.MaxPinAge, "MaxPinAge must be positive.");
        if (budgets.MaxPinnedBytes < 0)
            throw new ArgumentOutOfRangeException(parameter, budgets.MaxPinnedBytes, "MaxPinnedBytes must not be negative.");
        if (budgets.MaxBytesPerSecond <= 0)
            throw new ArgumentOutOfRangeException(parameter, budgets.MaxBytesPerSecond, "MaxBytesPerSecond must be positive.");
        if (budgets.MaxConcurrentObjects <= 0)
            throw new ArgumentOutOfRangeException(parameter, budgets.MaxConcurrentObjects, "MaxConcurrentObjects must be positive.");
    }

    /// <summary>Validates a backup schedule.</summary>
    public static void Schedule(DfsBackupSchedule? schedule, string parameter = "schedule")
    {
        if (schedule is null)
            throw new ArgumentNullException(parameter);
        if (schedule.Interval is { } interval && interval < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(parameter, interval, "The schedule interval must not be negative.");
    }
}
