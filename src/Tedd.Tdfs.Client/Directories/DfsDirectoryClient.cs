using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>Directory operations of a volume (thread-safe).</summary>
public sealed class DfsDirectoryClient
{
    internal DfsDirectoryClient(DfsVolume volume, bool isReadOnly = false, DfsTransaction? transaction = null)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
        IsReadOnly = isReadOnly;
        Transaction = transaction;
    }

    /// <summary>The volume this facade operates on.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>The transaction this facade is bound to, if any.</summary>
    internal DfsTransaction? Transaction { get; }

    /// <summary>True for snapshot views and backup hosts; mutators throw <see cref="DfsReadOnlyException"/>.</summary>
    public bool IsReadOnly { get; }

    /// <summary>True when operations stage into a transaction and become visible only at its commit.</summary>
    public bool IsTransactionBound => Transaction is not null;

    /// <summary>Creates the directory and any missing ancestors; succeeds when it already exists.</summary>
    public Task<DfsDirectoryInfo> CreateDirectoryAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.CreateDirectoryAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CreateDirectoryAsync(string, CancellationToken)"/>.</summary>
    public DfsDirectoryInfo CreateDirectory(string path) =>
        SyncBridge.Run(() => CreateDirectoryAsync(path, CancellationToken.None));

    /// <summary>Creates the directory recursively with explicit operation options.</summary>
    public Task<DfsDirectoryInfo> CreateDirectoryAsync(string path, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.CreateDirectoryAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CreateDirectoryAsync(string, DfsMutationOptions, CancellationToken)"/>.</summary>
    public DfsDirectoryInfo CreateDirectory(string path, DfsMutationOptions options) =>
        SyncBridge.Run(() => CreateDirectoryAsync(path, options, CancellationToken.None));

    /// <summary>True when a directory exists at <paramref name="path"/>; false only for genuine absence.</summary>
    public Task<bool> ExistsAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.ExistsAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ExistsAsync"/>.</summary>
    public bool Exists(string path) =>
        SyncBridge.Run(() => ExistsAsync(path, CancellationToken.None));

    /// <summary>Reads directory metadata; <see cref="DirectoryNotFoundException"/> when absent.</summary>
    public Task<DfsDirectoryInfo> GetInfoAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.GetInfoAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsDirectoryInfo GetInfo(string path) =>
        SyncBridge.Run(() => GetInfoAsync(path, CancellationToken.None));

    /// <summary>Atomically renames a directory (moving it beneath itself is rejected).</summary>
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.MoveAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, CancellationToken.None));

    /// <summary>Atomically renames a directory with explicit operation options.</summary>
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.MoveAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath, DfsMutationOptions options) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, options, CancellationToken.None));

    /// <summary>
    /// Deletes a directory: <see cref="DirectoryNotFoundException"/> when absent; <see cref="DfsDirectoryNotEmptyException"/>
    /// when not empty and <paramref name="recursive"/> is false.
    /// </summary>
    public Task<CommitToken> DeleteAsync(string path, bool recursive = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.DeleteAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, bool, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path, bool recursive = false) =>
        SyncBridge.Run(() => DeleteAsync(path, recursive, CancellationToken.None));

    /// <summary>Deletes a directory with explicit operation options.</summary>
    public Task<CommitToken> DeleteAsync(string path, bool recursive, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.DeleteAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, bool, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path, bool recursive, DfsMutationOptions options) =>
        SyncBridge.Run(() => DeleteAsync(path, recursive, options, CancellationToken.None));

    /// <summary>Enumerates the directory at one pinned version for the enumerator's lifetime.</summary>
    public IAsyncEnumerable<DfsEntryInfo> EnumerateAsync(string path, DfsEnumerationOptions? options = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.EnumerateAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="EnumerateAsync"/>; dispose the enumerator to release the pin.</summary>
    public IEnumerable<DfsEntryInfo> Enumerate(string path, DfsEnumerationOptions? options = null) =>
        SyncBridge.Enumerate(() => EnumerateAsync(path, options, CancellationToken.None));

    /// <summary>
    /// Lists one page. Pass the previous page's <see cref="DfsDirectoryPage.ContinuationToken"/> to continue at the same
    /// pinned version; an expired token fails with <see cref="DfsPinExpiredException"/>.
    /// </summary>
    public Task<DfsDirectoryPage> ListPageAsync(string path, DfsEnumerationOptions? options = null, string? continuationToken = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.ListPageAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ListPageAsync"/>.</summary>
    public DfsDirectoryPage ListPage(string path, DfsEnumerationOptions? options = null, string? continuationToken = null) =>
        SyncBridge.Run(() => ListPageAsync(path, options, continuationToken, CancellationToken.None));
}
