using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

// Addendum A: NTFS-like security descriptors (ACL-02..ACL-07) and named streams (ADS-03).
public sealed partial class DfsFileClient
{
    /// <summary>
    /// Reads the security descriptor of a file (owner, protection, explicit entries, then inherited entries in evaluation
    /// order). A stream path reads its file's descriptor.
    /// </summary>
    public Task<DfsSecurityDescriptor> GetAccessControlAsync(string path, CancellationToken ct = default) =>
        SecurityOperations.GetAccessControlAsync(Volume, Begin, Validation.Path, path, Pin, TxId, Bound, ct, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="GetAccessControlAsync"/>.</summary>
    public DfsSecurityDescriptor GetAccessControl(string path) =>
        SyncBridge.Run(() => GetAccessControlAsync(path, CancellationToken.None));

    /// <summary>
    /// Atomically replaces the explicit entries and protection flag of a file (inherited entries and the owner in
    /// <paramref name="descriptor"/> are ignored; change the owner with <see cref="SetOwnerAsync"/>).
    /// </summary>
    public Task<CommitToken> SetAccessControlAsync(string path, DfsSecurityDescriptor descriptor, CancellationToken ct = default) =>
        SecurityOperations.SetAccessControlAsync(Volume, Begin, Validation.Path, path, descriptor, TxId, Bound, ct, EnsureWritable, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="SetAccessControlAsync"/>.</summary>
    public CommitToken SetAccessControl(string path, DfsSecurityDescriptor descriptor) =>
        SyncBridge.Run(() => SetAccessControlAsync(path, descriptor, CancellationToken.None));

    /// <summary>Changes the owner of a file (TakeOwnership for oneself, Administrators for any owner).</summary>
    public Task<CommitToken> SetOwnerAsync(string path, DfsIdentity owner, CancellationToken ct = default) =>
        SecurityOperations.SetOwnerAsync(Volume, Begin, Validation.Path, path, owner, TxId, Bound, ct, EnsureWritable, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="SetOwnerAsync"/>.</summary>
    public CommitToken SetOwner(string path, DfsIdentity owner) =>
        SyncBridge.Run(() => SetOwnerAsync(path, owner, CancellationToken.None));

    /// <summary>The rights <paramref name="identity"/> (null = the caller) would be granted on a file.</summary>
    public Task<DfsFileSystemRights> GetEffectiveRightsAsync(string path, DfsIdentity? identity = null, CancellationToken ct = default) =>
        SecurityOperations.GetEffectiveRightsAsync(Volume, Begin, Validation.Path, path, identity, Pin, TxId, Bound, ct, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="GetEffectiveRightsAsync"/>.</summary>
    public DfsFileSystemRights GetEffectiveRights(string path, DfsIdentity? identity = null) =>
        SyncBridge.Run(() => GetEffectiveRightsAsync(path, identity, CancellationToken.None));

    /// <summary>Lists the named streams of a file or directory (ordinal by name; the default stream is not listed).</summary>
    public async Task<IReadOnlyList<DfsStreamInfo>> ListStreamsAsync(string path, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.GetInfo, path, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.Path(path);
        if (DfsStreamPath.HasStream(p))
            throw DfsPath.InvalidPath($"'{Validation.Printable(p)}' addresses a stream; list the streams of its file instead.", nameof(path));
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        ListStreamsResultMessage result = await Volume.Context.ReadAsync<ListStreamsResultMessage>(scope, "ListStreams",
            header => new ListStreamsMessage(header, p, Pin), p, TxId, requireSession: Pin != 0 || TxId is not null,
            binding: Bound).ConfigureAwait(false);
        return result.Streams.Select(s => new DfsStreamInfo(s.Name, s.Length, s.Revision)).ToArray();
    }

    /// <summary>Synchronous counterpart of <see cref="ListStreamsAsync"/>.</summary>
    public IReadOnlyList<DfsStreamInfo> ListStreams(string path) =>
        SyncBridge.Run(() => ListStreamsAsync(path, CancellationToken.None));
}

/// <summary>Security-descriptor operations shared by the file and directory facades (Addendum A, ACL-02..07).</summary>
internal static class SecurityOperations
{
    public static async Task<DfsSecurityDescriptor> GetAccessControlAsync(DfsVolume volume, Func<DfsOperationKind, CancellationToken, OperationScope> begin,
        Func<string?, string, string> validate, string path, ulong pin, DfsTransactionId? transaction, VolumeBinding? binding, CancellationToken ct, Func<IDisposable?> enterTransaction)
    {
        using OperationScope scope = begin(DfsOperationKind.GetInfo, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = validate(path, nameof(path));
        using IDisposable? txOperation = enterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        GetAccessControlResultMessage result = await volume.Context.ReadAsync<GetAccessControlResultMessage>(scope, "GetAccessControl",
            header => new GetAccessControlMessage(header, p, pin), p, transaction, requireSession: pin != 0 || transaction is not null, binding: binding).ConfigureAwait(false);
        return result.Descriptor;
    }

    public static async Task<CommitToken> SetAccessControlAsync(DfsVolume volume, Func<DfsOperationKind, CancellationToken, OperationScope> begin,
        Func<string?, string, string> validate, string path, DfsSecurityDescriptor descriptor, DfsTransactionId? transaction, VolumeBinding? binding, CancellationToken ct,
        Action ensureWritable, Func<IDisposable?> enterTransaction)
    {
        using OperationScope scope = begin(DfsOperationKind.Admin, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = validate(path, nameof(path));
        ArgumentNullException.ThrowIfNull(descriptor);
        foreach (DfsAccessRule rule in descriptor.ExplicitRules)
        {
            if (rule.GetValidationError() is { } invalid)
                throw new ArgumentException($"Invalid security descriptor entry: {invalid}", nameof(descriptor));
        }
        ensureWritable();
        using IDisposable? txOperation = enterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        SetAccessControlResultMessage result = await volume.Context.MutateAsync<SetAccessControlResultMessage>(scope, "SetAccessControl", null,
            header => new SetAccessControlMessage(header, p, descriptor), p, transaction, binding: binding).ConfigureAwait(false);
        return result.Token;
    }

    public static async Task<CommitToken> SetOwnerAsync(DfsVolume volume, Func<DfsOperationKind, CancellationToken, OperationScope> begin,
        Func<string?, string, string> validate, string path, DfsIdentity owner, DfsTransactionId? transaction, VolumeBinding? binding, CancellationToken ct, Action ensureWritable,
        Func<IDisposable?> enterTransaction)
    {
        using OperationScope scope = begin(DfsOperationKind.Admin, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = validate(path, nameof(path));
        if (owner.IsDefault)
            throw new ArgumentException("The owner identity must not be empty.", nameof(owner));
        if (owner == DfsIdentity.CreatorOwner)
            throw new ArgumentException("CreatorOwner is a placeholder for inheritable entries and cannot own an inode.", nameof(owner));
        ensureWritable();
        using IDisposable? txOperation = enterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        SetOwnerResultMessage result = await volume.Context.MutateAsync<SetOwnerResultMessage>(scope, "SetOwner", null,
            header => new SetOwnerMessage(header, p, owner), p, transaction, binding: binding).ConfigureAwait(false);
        return result.Token;
    }

    public static async Task<DfsFileSystemRights> GetEffectiveRightsAsync(DfsVolume volume, Func<DfsOperationKind, CancellationToken, OperationScope> begin,
        Func<string?, string, string> validate, string path, DfsIdentity? identity, ulong pin, DfsTransactionId? transaction, VolumeBinding? binding, CancellationToken ct,
        Func<IDisposable?> enterTransaction)
    {
        using OperationScope scope = begin(DfsOperationKind.GetInfo, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = validate(path, nameof(path));
        using IDisposable? txOperation = enterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        GetEffectiveRightsResultMessage result = await volume.Context.ReadAsync<GetEffectiveRightsResultMessage>(scope, "GetEffectiveRights",
            header => new GetEffectiveRightsMessage(header, p, identity, pin), p, transaction, requireSession: pin != 0 || transaction is not null, binding: binding)
            .ConfigureAwait(false);
        return result.Rights;
    }
}
