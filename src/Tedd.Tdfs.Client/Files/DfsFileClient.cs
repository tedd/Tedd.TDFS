using System.Text;
using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>
/// File operations of a volume (thread-safe). Whole-file helpers are atomic and durable on return and preserve inode
/// identity; when <see cref="IsTransactionBound"/> they stage into the transaction and return <see cref="CommitToken.None"/>.
/// </summary>
public sealed class DfsFileClient
{
    internal DfsFileClient(DfsVolume volume, bool isReadOnly = false, DfsTransaction? transaction = null)
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

    // ---- Open family ------------------------------------------------------------------------------------------

    /// <summary>Opens a stream on <paramref name="path"/> with full control over mode, access, sharing and views.</summary>
    public Task<DfsStream> OpenAsync(string path, DfsOpenOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.OpenAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="OpenAsync(string, DfsOpenOptions, CancellationToken)"/>.</summary>
    public DfsStream Open(string path, DfsOpenOptions options) =>
        SyncBridge.Run(() => OpenAsync(path, options, CancellationToken.None));

    /// <summary>Opens a stream with the given mode, access and sharing (live reads, flush-published writes).</summary>
    public Task<DfsStream> OpenAsync(string path, FileMode mode, FileAccess access, FileShare share, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.OpenAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="OpenAsync(string, FileMode, FileAccess, FileShare, CancellationToken)"/>.</summary>
    public DfsStream Open(string path, FileMode mode, FileAccess access, FileShare share) =>
        SyncBridge.Run(() => OpenAsync(path, mode, access, share, CancellationToken.None));

    /// <summary>Opens an existing file for reading (Open, Read, Share.Read, Live).</summary>
    public Task<DfsStream> OpenReadAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.OpenReadAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="OpenReadAsync"/>.</summary>
    public DfsStream OpenRead(string path) =>
        SyncBridge.Run(() => OpenReadAsync(path, CancellationToken.None));

    /// <summary>Opens or creates a file for writing at offset zero without truncation (OpenOrCreate, Write, Share.Read).</summary>
    public Task<DfsStream> OpenWriteAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.OpenWriteAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="OpenWriteAsync"/>.</summary>
    public DfsStream OpenWrite(string path) =>
        SyncBridge.Run(() => OpenWriteAsync(path, CancellationToken.None));

    /// <summary>Creates or truncates a file (Create, ReadWrite, Share.Read).</summary>
    public Task<DfsStream> CreateAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.CreateAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CreateAsync"/>.</summary>
    public DfsStream Create(string path) =>
        SyncBridge.Run(() => CreateAsync(path, CancellationToken.None));

    // ---- Whole-file reads -------------------------------------------------------------------------------------

    /// <summary>Reads one pinned revision of the file (bounded by <see cref="DfsClientOptions.MaxMaterializeBytes"/>).</summary>
    public Task<byte[]> ReadAllBytesAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.ReadAllBytesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ReadAllBytesAsync"/>.</summary>
    public byte[] ReadAllBytes(string path) =>
        SyncBridge.Run(() => ReadAllBytesAsync(path, CancellationToken.None));

    /// <summary>Reads one pinned revision as text (UTF-8 by default; a UTF-8 BOM is recognised).</summary>
    public Task<string> ReadAllTextAsync(string path, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.ReadAllTextAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ReadAllTextAsync"/>.</summary>
    public string ReadAllText(string path, Encoding? encoding = null) =>
        SyncBridge.Run(() => ReadAllTextAsync(path, encoding, CancellationToken.None));

    /// <summary>Reads one pinned revision as lines.</summary>
    public Task<string[]> ReadAllLinesAsync(string path, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.ReadAllLinesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ReadAllLinesAsync"/>.</summary>
    public string[] ReadAllLines(string path, Encoding? encoding = null) =>
        SyncBridge.Run(() => ReadAllLinesAsync(path, encoding, CancellationToken.None));

    /// <summary>Streams the lines of one revision pinned for the enumerator's lifetime.</summary>
    public IAsyncEnumerable<string> ReadLinesAsync(string path, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.ReadLinesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ReadLinesAsync"/>; dispose the enumerator to release the pin.</summary>
    public IEnumerable<string> ReadLines(string path, Encoding? encoding = null) =>
        SyncBridge.Enumerate(() => ReadLinesAsync(path, encoding, CancellationToken.None));

    // ---- Whole-file writes ------------------------------------------------------------------------------------

    /// <summary>Atomically replaces (or creates) the file's contents.</summary>
    public Task<CommitToken> WriteAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.WriteAllBytesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="WriteAllBytesAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    public CommitToken WriteAllBytes(string path, ReadOnlyMemory<byte> bytes) =>
        SyncBridge.Run(() => WriteAllBytesAsync(path, bytes, CancellationToken.None));

    /// <summary>Atomically replaces (or creates) the file's contents with explicit operation options.</summary>
    public Task<CommitToken> WriteAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.WriteAllBytesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="WriteAllBytesAsync(string, ReadOnlyMemory{byte}, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken WriteAllBytes(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options) =>
        SyncBridge.Run(() => WriteAllBytesAsync(path, bytes, options, CancellationToken.None));

    /// <summary>Atomically replaces the file's contents with text (UTF-8 without BOM by default).</summary>
    public Task<CommitToken> WriteAllTextAsync(string path, string contents, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.WriteAllTextAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="WriteAllTextAsync"/>.</summary>
    public CommitToken WriteAllText(string path, string contents, Encoding? encoding = null) =>
        SyncBridge.Run(() => WriteAllTextAsync(path, contents, encoding, CancellationToken.None));

    /// <summary>Atomically replaces the file's contents with lines.</summary>
    public Task<CommitToken> WriteAllLinesAsync(string path, IEnumerable<string> lines, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.WriteAllLinesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="WriteAllLinesAsync"/>.</summary>
    public CommitToken WriteAllLines(string path, IEnumerable<string> lines, Encoding? encoding = null) =>
        SyncBridge.Run(() => WriteAllLinesAsync(path, lines, encoding, CancellationToken.None));

    /// <summary>Atomically appends one complete suffix under one operation id (creates the file when absent).</summary>
    public Task<CommitToken> AppendAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.AppendAllBytesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AppendAllBytesAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    public CommitToken AppendAllBytes(string path, ReadOnlyMemory<byte> bytes) =>
        SyncBridge.Run(() => AppendAllBytesAsync(path, bytes, CancellationToken.None));

    /// <summary>Atomically appends one complete suffix with explicit operation options.</summary>
    public Task<CommitToken> AppendAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.AppendAllBytesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AppendAllBytesAsync(string, ReadOnlyMemory{byte}, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken AppendAllBytes(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options) =>
        SyncBridge.Run(() => AppendAllBytesAsync(path, bytes, options, CancellationToken.None));

    /// <summary>Atomically appends text.</summary>
    public Task<CommitToken> AppendAllTextAsync(string path, string contents, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.AppendAllTextAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AppendAllTextAsync"/>.</summary>
    public CommitToken AppendAllText(string path, string contents, Encoding? encoding = null) =>
        SyncBridge.Run(() => AppendAllTextAsync(path, contents, encoding, CancellationToken.None));

    /// <summary>Atomically appends lines.</summary>
    public Task<CommitToken> AppendAllLinesAsync(string path, IEnumerable<string> lines, Encoding? encoding = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.AppendAllLinesAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AppendAllLinesAsync"/>.</summary>
    public CommitToken AppendAllLines(string path, IEnumerable<string> lines, Encoding? encoding = null) =>
        SyncBridge.Run(() => AppendAllLinesAsync(path, lines, encoding, CancellationToken.None));

    // ---- Namespace operations ---------------------------------------------------------------------------------

    /// <summary>Atomically copies one pinned source revision to the destination (objects are shared).</summary>
    public Task<CommitToken> CopyAsync(string sourcePath, string destinationPath, bool overwrite = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.CopyAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CopyAsync(string, string, bool, CancellationToken)"/>.</summary>
    public CommitToken Copy(string sourcePath, string destinationPath, bool overwrite = false) =>
        SyncBridge.Run(() => CopyAsync(sourcePath, destinationPath, overwrite, CancellationToken.None));

    /// <summary>Atomically copies with explicit operation options.</summary>
    public Task<CommitToken> CopyAsync(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.CopyAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CopyAsync(string, string, bool, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Copy(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options) =>
        SyncBridge.Run(() => CopyAsync(sourcePath, destinationPath, overwrite, options, CancellationToken.None));

    /// <summary>Atomically renames a file (inode identity is preserved).</summary>
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, bool overwrite = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.MoveAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, bool, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath, bool overwrite = false) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, overwrite, CancellationToken.None));

    /// <summary>Atomically renames a file with explicit operation options.</summary>
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.MoveAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, bool, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, overwrite, options, CancellationToken.None));

    /// <summary>Deletes a file; deleting an absent file succeeds.</summary>
    public Task<CommitToken> DeleteAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.DeleteAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path) =>
        SyncBridge.Run(() => DeleteAsync(path, CancellationToken.None));

    /// <summary>Deletes a file with explicit operation options; deleting an absent file succeeds.</summary>
    public Task<CommitToken> DeleteAsync(string path, DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.DeleteAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path, DfsMutationOptions options) =>
        SyncBridge.Run(() => DeleteAsync(path, options, CancellationToken.None));

    /// <summary>
    /// True when a file exists at <paramref name="path"/>; false only for genuine absence (authorization,
    /// connectivity and integrity failures propagate).
    /// </summary>
    public Task<bool> ExistsAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.ExistsAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ExistsAsync"/>.</summary>
    public bool Exists(string path) =>
        SyncBridge.Run(() => ExistsAsync(path, CancellationToken.None));

    /// <summary>Reads file metadata; <see cref="FileNotFoundException"/> when absent.</summary>
    public Task<DfsFileInfo> GetInfoAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.GetInfoAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsFileInfo GetInfo(string path) =>
        SyncBridge.Run(() => GetInfoAsync(path, CancellationToken.None));

    /// <summary>Changes the file's named storage policy (data is migrated in the background).</summary>
    public Task<CommitToken> SetStoragePolicyAsync(string path, string policyName, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.SetStoragePolicyAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetStoragePolicyAsync"/>.</summary>
    public CommitToken SetStoragePolicy(string path, string policyName) =>
        SyncBridge.Run(() => SetStoragePolicyAsync(path, policyName, CancellationToken.None));

    /// <summary>Changes the inode access flags (owner or administrators only).</summary>
    public Task<CommitToken> SetAccessAsync(string path, DfsInodeAccess access, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.SetAccessAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetAccessAsync"/>.</summary>
    public CommitToken SetAccess(string path, DfsInodeAccess access) =>
        SyncBridge.Run(() => SetAccessAsync(path, access, CancellationToken.None));
}
