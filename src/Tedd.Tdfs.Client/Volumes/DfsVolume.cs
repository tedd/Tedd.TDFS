using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>A volume of the connected cluster: entry point to its file, directory, transaction, snapshot and backup facades.</summary>
public sealed class DfsVolume
{
    internal DfsVolume(DfsClient client, string name, bool isReadOnly = false)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(name);
        Client = client;
        Name = name;
        IsReadOnly = isReadOnly;
        Context = client.Runtime.GetVolumeContext(name);
        Files = new DfsFileClient(this, isReadOnly);
        Directories = new DfsDirectoryClient(this, isReadOnly);
        Transactions = new DfsTransactionClient(this);
        Snapshots = new DfsSnapshotClient(this, isReadOnly);
        Backups = new DfsBackupClient(this);
    }

    /// <summary>True when the volume is served read-only (backup hosts).</summary>
    internal bool IsReadOnly { get; }

    /// <summary>Per-client state of the volume (routing, session, leases).</summary>
    internal VolumeContext Context { get; }

    /// <summary>The client runtime.</summary>
    internal ClientRuntime Runtime => Client.Runtime;

    /// <summary>Volume name.</summary>
    public string Name { get; }

    /// <summary>The owning client.</summary>
    public DfsClient Client { get; }

    /// <summary>File operations.</summary>
    public DfsFileClient Files { get; }

    /// <summary>Directory operations.</summary>
    public DfsDirectoryClient Directories { get; }

    /// <summary>Transactions.</summary>
    public DfsTransactionClient Transactions { get; }

    /// <summary>Snapshots.</summary>
    public DfsSnapshotClient Snapshots { get; }

    /// <summary>Late-sync backup targets.</summary>
    public DfsBackupClient Backups { get; }

    /// <summary>Reads the volume's metadata (from its leader, with the last commit).</summary>
    public async Task<DfsVolumeInfo> GetInfoAsync(CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.GetInfo, Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        await scope.ValidatedAsync().ConfigureAwait(false);
        var request = new RpcRequest
        {
            Name = "GetVolumeRoute",
            ReplyType = MessageType.VolumeRouteResult,
            Kind = CallKind.Read,
            Router = Context,
            Build = _ => ValueTask.FromResult<IWireMessage>(new GetVolumeRouteMessage(Name, RequireLeader: !Runtime.IsBackupHost)),
        };
        var route = (VolumeRouteResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
        Context.Bind(route.Volume);
        return WireConvert.ToVolumeInfo(route.Volume, route.LastCommit);
    }

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsVolumeInfo GetInfo() =>
        SyncBridge.Run(() => GetInfoAsync(CancellationToken.None));

    /// <summary>
    /// Reserves an operation id in this client's session, for persisting before use (FSD §13). The id is never
    /// acknowledged by this client, so its outcome stays queryable (within the volume's retention bounds) even from
    /// another process.
    /// </summary>
    public async Task<DfsOperationId> NewOperationIdAsync(CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.OpenSession, Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        await scope.ValidatedAsync().ConfigureAwait(false);
        return await Context.ReserveApplicationIdAsync(scope).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="NewOperationIdAsync"/>.</summary>
    public DfsOperationId NewOperationId() =>
        SyncBridge.Run(() => NewOperationIdAsync(CancellationToken.None));

    /// <summary>
    /// Queries the outcome of an operation. With <paramref name="resolve"/> the volume fences an unfinished operation so
    /// the answer is definite (Committed or Aborted).
    /// </summary>
    public async Task<DfsOperationOutcome> QueryOperationAsync(DfsOperationId id, bool resolve = false, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.QueryOperation, Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        if (id.IsEmpty)
            throw new ArgumentException("The operation id must not be empty.", nameof(id));
        await scope.ValidatedAsync().ConfigureAwait(false);
        QueryOperationResultMessage result = await Context.QueryAsync(id, isTransaction: false, resolve, scope).ConfigureAwait(false);
        return WireConvert.ToOutcome(id, result);
    }

    /// <summary>Synchronous counterpart of <see cref="QueryOperationAsync"/>.</summary>
    public DfsOperationOutcome QueryOperation(DfsOperationId id, bool resolve = false) =>
        SyncBridge.Run(() => QueryOperationAsync(id, resolve, CancellationToken.None));
}
