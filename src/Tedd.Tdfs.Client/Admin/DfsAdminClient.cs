using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// Cluster administration (FSD §14, PROTOCOL.md §8.8): nodes, volumes, policies, principals, keys, scrub/repair, restore
/// from backup and features. Requests go to the control leader (or the volume leader for volume-scoped operations, or
/// any node for listings) and are deduplicated by a request id across retries. Every change is audited by the cluster;
/// secrets never appear in SDK diagnostics.
/// </summary>
public sealed partial class DfsAdminClient
{
    internal DfsAdminClient(DfsClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        Client = client;
    }

    /// <summary>The owning client.</summary>
    internal DfsClient Client { get; }

    private ClientRuntime Runtime => Client.Runtime;

    /// <summary>Status of the cluster: control group, members, volumes, placement epoch and active features.</summary>
    public async Task<DfsClusterStatus> GetClusterStatusAsync(CancellationToken ct = default)
    {
        AdminClusterStatusBody body = await CallAsync<AdminEmptyBody, AdminClusterStatusBody>(AdminOp.ClusterStatus, () => new AdminEmptyBody(),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return new DfsClusterStatus(new ClusterIdentity(body.ClusterId, body.ClusterName), WireConvert.ToGroupStatus(body.Control),
            body.Nodes.Select(WireConvert.ToNodeInfo).ToArray(), body.Volumes, body.PlacementEpoch, body.ActiveFeatures);
    }

    /// <summary>Lists the cluster members.</summary>
    public async Task<IReadOnlyList<DfsNodeInfo>> ListNodesAsync(CancellationToken ct = default)
    {
        AdminNodeListBody body = await CallAsync<AdminEmptyBody, AdminNodeListBody>(AdminOp.ListNodes, () => new AdminEmptyBody(), Runtime.ControlRouter,
            DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return body.Nodes.Select(WireConvert.ToNodeInfo).ToArray();
    }

    /// <summary>Pre-registers a node (it joins when it starts with matching identity).</summary>
    public async Task<DfsNodeInfo> AddNodeAsync(string name, DfsTopologyLabels labels, IReadOnlyList<string> endpoints, CancellationToken ct = default)
    {
        NodeEntry node = await CallAsync<AdminAddNodeBody, NodeEntry>(AdminOp.AddNode, () =>
        {
            Validation.Name(name, nameof(name));
            ArgumentNullException.ThrowIfNull(labels);
            ArgumentNullException.ThrowIfNull(endpoints);
            if (endpoints.Count == 0 || endpoints.Any(string.IsNullOrEmpty))
                throw new ArgumentException("At least one non-empty endpoint is required.", nameof(endpoints));
            return new AdminAddNodeBody(name, labels, endpoints);
        }, Runtime.ControlRouter, DfsOperationKind.MembershipChange, ct).ConfigureAwait(false);
        return WireConvert.ToNodeInfo(node);
    }

    /// <summary>Drains a node: new placements avoid it and its data is re-replicated elsewhere.</summary>
    public Task DrainNodeAsync(string name, CancellationToken ct = default) =>
        CallAsync<AdminNodeNameBody, AdminEmptyBody>(AdminOp.DrainNode, () => new AdminNodeNameBody(Validation.Name(name, nameof(name))),
            Runtime.ControlRouter, DfsOperationKind.MembershipChange, ct);

    /// <summary>Removes a node (after draining unless <paramref name="force"/>).</summary>
    public Task RemoveNodeAsync(string name, bool force = false, CancellationToken ct = default) =>
        CallAsync<AdminRemoveNodeBody, AdminEmptyBody>(AdminOp.RemoveNode, () => new AdminRemoveNodeBody(Validation.Name(name, nameof(name)), force),
            Runtime.ControlRouter, DfsOperationKind.MembershipChange, ct);

    /// <summary>Changes a node's topology labels (placement epoch advances).</summary>
    public Task SetNodeLabelsAsync(string name, DfsTopologyLabels labels, CancellationToken ct = default) =>
        CallAsync<AdminSetNodeLabelsBody, AdminEmptyBody>(AdminOp.SetNodeLabels, () =>
        {
            Validation.Name(name, nameof(name));
            ArgumentNullException.ThrowIfNull(labels);
            return new AdminSetNodeLabelsBody(name, labels);
        }, Runtime.ControlRouter, DfsOperationKind.MembershipChange, ct);

    /// <summary>Creates a volume.</summary>
    public async Task<DfsVolumeInfo> CreateVolumeAsync(DfsVolumeSpec spec, CancellationToken ct = default)
    {
        AdminVolumeInfoBody body = await CallAsync<AdminVolumeSpecBody, AdminVolumeInfoBody>(AdminOp.CreateVolume, () =>
        {
            ArgumentNullException.ThrowIfNull(spec);
            Validation.Name(spec.Name, nameof(spec));
            if (!DfsPath.TryNormalizeComponent(spec.Name, out _, out string? error) || spec.Name.Contains(DfsPath.StreamSeparator, StringComparison.Ordinal))
                throw new ArgumentException($"Invalid volume name: {error ?? "':' is not allowed"}", nameof(spec));
            ArgumentNullException.ThrowIfNull(spec.Metadata, nameof(spec));
            ArgumentNullException.ThrowIfNull(spec.DefaultPolicy, nameof(spec));
            ArgumentNullException.ThrowIfNull(spec.NamedPolicies, nameof(spec));
            ArgumentNullException.ThrowIfNull(spec.Limits, nameof(spec));
            if (spec.Metadata.GetValidationError() is { } metadata)
                throw new ArgumentException($"Invalid metadata policy: {metadata}", nameof(spec));
            if (spec.DefaultPolicy.GetValidationError() is { } policy)
                throw new ArgumentException($"Invalid default storage policy: {policy}", nameof(spec));
            foreach ((string policyName, DfsStoragePolicy named) in spec.NamedPolicies)
            {
                if (string.IsNullOrEmpty(policyName))
                    throw new ArgumentException("Named policies need non-empty names.", nameof(spec));
                if (named?.GetValidationError() is { } invalid)
                    throw new ArgumentException($"Invalid storage policy '{policyName}': {invalid}", nameof(spec));
            }

            if (spec.InlineThreshold < 0)
                throw new ArgumentOutOfRangeException(nameof(spec), spec.InlineThreshold, "The inline threshold must not be negative.");
            return new AdminVolumeSpecBody(spec.Name, spec.Metadata, spec.DefaultPolicy, spec.NamedPolicies, spec.InlineThreshold, spec.Limits)
            {
                StoragePool = string.IsNullOrEmpty(spec.StoragePool) ? DfsPoolNames.Default : spec.StoragePool,
                RootSecurity = spec.RootSecurity,
                EnforceTraverseChecking = spec.EnforceTraverseChecking,
            };
        }, Runtime.ControlRouter, DfsOperationKind.Admin, ct).ConfigureAwait(false);
        Runtime.GetVolumeContext(body.Volume.Name).Bind(body.Volume);
        return WireConvert.ToVolumeInfo(body.Volume, body.LastCommit);
    }

    /// <summary>Deletes a volume (its name can later designate a new, unrelated volume).</summary>
    public Task DeleteVolumeAsync(string name, CancellationToken ct = default) =>
        CallAsync<AdminVolumeNameBody, AdminEmptyBody>(AdminOp.DeleteVolume, () => new AdminVolumeNameBody(Validation.Name(name, nameof(name))),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Names of the cluster's volumes.</summary>
    public async Task<IReadOnlyList<string>> ListVolumesAsync(CancellationToken ct = default)
    {
        AdminNameListBody body = await CallAsync<AdminEmptyBody, AdminNameListBody>(AdminOp.ListVolumes, () => new AdminEmptyBody(), Runtime.AnyNodeRouter,
            DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return body.Names;
    }

    /// <summary>Status of a volume: metadata group, replica health and backup targets.</summary>
    public async Task<DfsVolumeStatus> GetVolumeStatusAsync(string name, CancellationToken ct = default)
    {
        Validation.Name(name, nameof(name));
        AdminVolumeStatusBody body = await CallAsync<AdminVolumeNameBody, AdminVolumeStatusBody>(AdminOp.VolumeStatus, () => new AdminVolumeNameBody(name),
            Runtime.GetVolumeContext(name), DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return new DfsVolumeStatus(WireConvert.ToVolumeInfo(body.Volume, body.LastCommit), WireConvert.ToGroupStatus(body.Group),
            WireConvert.ToReplicaHealth(body.Replicas), body.Backups.Select(WireConvert.ToBackupStatus).ToArray());
    }

    /// <summary>Changes a volume's default storage policy (optionally migrating existing data).</summary>
    public Task SetDefaultPolicyAsync(string volume, DfsStoragePolicy policy, bool migrateExisting, CancellationToken ct = default) =>
        CallAsync<AdminSetDefaultPolicyBody, AdminEmptyBody>(AdminOp.SetDefaultPolicy, () =>
        {
            Validation.Name(volume, nameof(volume));
            ArgumentNullException.ThrowIfNull(policy);
            if (policy.GetValidationError() is { } invalid)
                throw new ArgumentException($"Invalid storage policy: {invalid}", nameof(policy));
            return new AdminSetDefaultPolicyBody(volume, policy, migrateExisting);
        }, Runtime.ControlRouter, DfsOperationKind.PolicyChange, ct);

    /// <summary>Defines (or redefines) a named storage policy of a volume.</summary>
    public Task DefinePolicyAsync(string volume, string policyName, DfsStoragePolicy policy, CancellationToken ct = default) =>
        CallAsync<AdminDefinePolicyBody, AdminEmptyBody>(AdminOp.DefinePolicy, () =>
        {
            Validation.Name(volume, nameof(volume));
            Validation.Name(policyName, nameof(policyName));
            ArgumentNullException.ThrowIfNull(policy);
            if (policy.GetValidationError() is { } invalid)
                throw new ArgumentException($"Invalid storage policy: {invalid}", nameof(policy));
            return new AdminDefinePolicyBody(volume, policyName, policy);
        }, Runtime.ControlRouter, DfsOperationKind.PolicyChange, ct);

    /// <summary>Progress of a policy migration.</summary>
    public async Task<DfsPolicyMigrationStatus> GetPolicyMigrationStatusAsync(string volume, CancellationToken ct = default)
    {
        Validation.Name(volume, nameof(volume));
        AdminPolicyMigrationBody body = await CallAsync<AdminVolumeNameBody, AdminPolicyMigrationBody>(AdminOp.PolicyMigrationStatus,
            () => new AdminVolumeNameBody(volume), Runtime.GetVolumeContext(volume), DfsOperationKind.PolicyChange, ct).ConfigureAwait(false);
        return new DfsPolicyMigrationStatus(body.Volume, body.Target, body.Effective, body.PendingObjects);
    }

    /// <summary>Creates a principal (the secret is sent once over the authenticated channel and never logged).</summary>
    public Task CreatePrincipalAsync(string principal, string secret, bool clusterAdmin = false, CancellationToken ct = default) =>
        CallAsync<AdminCreatePrincipalBody, AdminEmptyBody>(AdminOp.CreatePrincipal, () =>
        {
            Validation.Name(principal, nameof(principal));
            if (string.IsNullOrEmpty(secret))
                throw new ArgumentException("The secret must not be empty.", nameof(secret));
            return new AdminCreatePrincipalBody(principal, secret, clusterAdmin);
        }, Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Deletes a principal.</summary>
    public Task DeletePrincipalAsync(string principal, CancellationToken ct = default) =>
        CallAsync<AdminPrincipalNameBody, AdminEmptyBody>(AdminOp.DeletePrincipal, () => new AdminPrincipalNameBody(Validation.Name(principal, nameof(principal))),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Grants volume permissions to a principal.</summary>
    public Task GrantAsync(string volume, string principal, DfsPermissions permissions, CancellationToken ct = default) =>
        CallAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Grant, () =>
            new AdminGrantBody(Validation.Name(volume, nameof(volume)), Validation.Name(principal, nameof(principal)), permissions),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Revokes volume permissions of a principal.</summary>
    public Task RevokeAsync(string volume, string principal, DfsPermissions permissions, CancellationToken ct = default) =>
        CallAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Revoke, () =>
            new AdminGrantBody(Validation.Name(volume, nameof(volume)), Validation.Name(principal, nameof(principal)), permissions),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Rotates the volume's data encryption key.</summary>
    public Task RotateDataKeyAsync(string volume, CancellationToken ct = default) =>
        CallAsync<AdminVolumeNameBody, AdminKeyRotationBody>(AdminOp.RotateDataKey, () => new AdminVolumeNameBody(Validation.Name(volume, nameof(volume))),
            Runtime.ControlRouter, DfsOperationKind.KeyRotation, ct);

    /// <summary>Exports the cluster recovery key, encrypted with <paramref name="passphrase"/>.</summary>
    public async Task<byte[]> ExportRecoveryKeyAsync(string passphrase, CancellationToken ct = default)
    {
        AdminRecoveryKeyBody body = await CallAsync<AdminExportRecoveryKeyBody, AdminRecoveryKeyBody>(AdminOp.ExportRecoveryKey, () =>
        {
            if (string.IsNullOrEmpty(passphrase))
                throw new ArgumentException("The passphrase must not be empty.", nameof(passphrase));
            return new AdminExportRecoveryKeyBody(passphrase);
        }, Runtime.ControlRouter, DfsOperationKind.KeyRotation, ct).ConfigureAwait(false);
        return body.Escrow.ToArray();
    }

    /// <summary>Starts a scrub of every node (or of <paramref name="node"/>).</summary>
    public Task StartScrubAsync(string? node = null, CancellationToken ct = default) =>
        CallAsync<AdminStartScrubBody, AdminEmptyBody>(AdminOp.StartScrub, () =>
        {
            if (node is { Length: 0 })
                throw new ArgumentException("The node name must not be empty (use null for every node).", nameof(node));
            return new AdminStartScrubBody(node);
        }, Runtime.ControlRouter, DfsOperationKind.Scrub, ct);

    /// <summary>Starts a repair pass of a volume.</summary>
    public Task StartRepairAsync(string volume, CancellationToken ct = default)
    {
        Validation.Name(volume, nameof(volume));
        return CallAsync<AdminVolumeNameBody, AdminEmptyBody>(AdminOp.StartRepair, () => new AdminVolumeNameBody(volume), Runtime.GetVolumeContext(volume),
            DfsOperationKind.Repair, ct);
    }

    /// <summary>Replica health of a volume.</summary>
    public async Task<DfsReplicaHealth> GetReplicaHealthAsync(string volume, CancellationToken ct = default)
    {
        Validation.Name(volume, nameof(volume));
        ReplicaHealthData data = await CallAsync<AdminVolumeNameBody, ReplicaHealthData>(AdminOp.ReplicaHealth, () => new AdminVolumeNameBody(volume),
            Runtime.GetVolumeContext(volume), DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return WireConvert.ToReplicaHealth(data);
    }

    /// <summary>Restores a published backup snapshot into a new volume of this cluster.</summary>
    public async Task<DfsVolumeInfo> RestoreFromBackupAsync(DfsRestoreFromBackupRequest request, CancellationToken ct = default)
    {
        AdminVolumeInfoBody body = await CallAsync<AdminRestoreFromBackupBody, AdminVolumeInfoBody>(AdminOp.RestoreFromBackup, () =>
        {
            ArgumentNullException.ThrowIfNull(request);
            if (request.BackupEndpoints is null || request.BackupEndpoints.Count == 0 || request.BackupEndpoints.Any(string.IsNullOrEmpty))
                throw new ArgumentException("At least one non-empty backup endpoint is required.", nameof(request));
            ArgumentNullException.ThrowIfNull(request.BackupCredentials, nameof(request));
            Validation.Name(request.SourceVolume, nameof(request));
            Validation.Name(request.SnapshotName, nameof(request));
            Validation.Name(request.NewVolumeName, nameof(request));
            return new AdminRestoreFromBackupBody(request.BackupEndpoints, request.BackupCredentials, request.SourceVolume, request.SnapshotName,
                request.NewVolumeName, request.RecoveryKey ?? ReadOnlyMemory<byte>.Empty);
        }, Runtime.ControlRouter, DfsOperationKind.RestoreSnapshot, ct).ConfigureAwait(false);
        Runtime.GetVolumeContext(body.Volume.Name).Bind(body.Volume);
        return WireConvert.ToVolumeInfo(body.Volume, body.LastCommit);
    }

    /// <summary>Enables or disables a protocol feature cluster-wide.</summary>
    public Task SetFeatureAsync(string feature, bool enabled, CancellationToken ct = default) =>
        CallAsync<AdminSetFeatureBody, AdminEmptyBody>(AdminOp.SetFeature, () => new AdminSetFeatureBody(Validation.Name(feature, nameof(feature)), enabled),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>The most recent audit records (newest last), without secrets.</summary>
    public async Task<IReadOnlyList<string>> GetAuditLogAsync(int max = 1000, CancellationToken ct = default)
    {
        AdminNameListBody body = await CallAsync<AdminAuditLogRequestBody, AdminNameListBody>(AdminOp.AuditLog, () =>
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(max);
            return new AdminAuditLogRequestBody(max);
        }, Runtime.AnyNodeRouter, DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return body.Names;
    }

    /// <summary>
    /// Sends one administrative request: validation (inside <paramref name="body"/>) runs after the operation start, the
    /// request id is fixed for every retry (the leader deduplicates it).
    /// </summary>
    private async Task<TResult> CallAsync<TBody, TResult>(AdminOp op, Func<TBody> body, IRpcRouter router, DfsOperationKind kind, CancellationToken ct)
        where TBody : IWireRecord<TBody>
        where TResult : IWireRecord<TResult>
    {
        using OperationScope scope = Runtime.BeginOperation(kind, null, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        TBody request = body();
        await scope.ValidatedAsync().ConfigureAwait(false);
        UInt128 requestId = Runtime.Random.NextUInt128();
        AdminRequestMessage message = AdminRequestMessage.Create(op, requestId, request);
        var call = new RpcRequest
        {
            Name = "Admin." + op,
            ReplyType = MessageType.AdminResult,
            Kind = CallKind.Idempotent,
            Router = router,
            Build = _ => ValueTask.FromResult<IWireMessage>(message),
        };
        var reply = (AdminResultMessage)await Runtime.Rpc.InvokeAsync(call, scope).ConfigureAwait(false);
        if (reply.Op != op)
            throw new DfsProtocolException($"Admin {op} was answered as {reply.Op}.");
        return reply.ReadBody<TResult>();
    }
}
