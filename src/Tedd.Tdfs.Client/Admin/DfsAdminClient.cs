namespace Tedd.Tdfs;

/// <summary>Cluster administration (requires administrative grants). Async-only.</summary>
public sealed partial class DfsAdminClient
{
    internal DfsAdminClient(DfsClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        Client = client;
    }

    /// <summary>The owning client.</summary>
    internal DfsClient Client { get; }

    /// <summary>Reads cluster-wide status.</summary>
    public Task<DfsClusterStatus> GetClusterStatusAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GetClusterStatusAsync: implemented in a later phase");

    /// <summary>Lists live members.</summary>
    public Task<IReadOnlyList<DfsNodeInfo>> ListNodesAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.ListNodesAsync: implemented in a later phase");

    /// <summary>Admits a new node.</summary>
    public Task<DfsNodeInfo> AddNodeAsync(string name, DfsTopologyLabels labels, IReadOnlyList<string> endpoints, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.AddNodeAsync: implemented in a later phase");

    /// <summary>Starts moving data and voter roles off a node.</summary>
    public Task DrainNodeAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.DrainNodeAsync: implemented in a later phase");

    /// <summary>Removes a node; without <paramref name="force"/> it must be drained.</summary>
    public Task RemoveNodeAsync(string name, bool force = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.RemoveNodeAsync: implemented in a later phase");

    /// <summary>Changes a node's topology labels.</summary>
    public Task SetNodeLabelsAsync(string name, DfsTopologyLabels labels, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.SetNodeLabelsAsync: implemented in a later phase");

    /// <summary>Creates a volume.</summary>
    public Task<DfsVolumeInfo> CreateVolumeAsync(DfsVolumeSpec spec, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.CreateVolumeAsync: implemented in a later phase");

    /// <summary>Deletes a volume.</summary>
    public Task DeleteVolumeAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.DeleteVolumeAsync: implemented in a later phase");

    /// <summary>Lists volume names.</summary>
    public Task<IReadOnlyList<string>> ListVolumesAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.ListVolumesAsync: implemented in a later phase");

    /// <summary>Reads a volume's administrative status.</summary>
    public Task<DfsVolumeStatus> GetVolumeStatusAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GetVolumeStatusAsync: implemented in a later phase");

    /// <summary>Changes the volume's default storage policy, optionally migrating existing data.</summary>
    public Task SetDefaultPolicyAsync(string volume, DfsStoragePolicy policy, bool migrateExisting, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.SetDefaultPolicyAsync: implemented in a later phase");

    /// <summary>Defines or replaces a named storage policy.</summary>
    public Task DefinePolicyAsync(string volume, string policyName, DfsStoragePolicy policy, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.DefinePolicyAsync: implemented in a later phase");

    /// <summary>Reads the progress of a policy migration.</summary>
    public Task<DfsPolicyMigrationStatus> GetPolicyMigrationStatusAsync(string volume, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GetPolicyMigrationStatusAsync: implemented in a later phase");

    /// <summary>Creates a principal.</summary>
    public Task CreatePrincipalAsync(string principal, string secret, bool clusterAdmin = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.CreatePrincipalAsync: implemented in a later phase");

    /// <summary>Deletes a principal.</summary>
    public Task DeletePrincipalAsync(string principal, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.DeletePrincipalAsync: implemented in a later phase");

    /// <summary>Grants volume permissions to a principal.</summary>
    public Task GrantAsync(string volume, string principal, DfsPermissions permissions, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GrantAsync: implemented in a later phase");

    /// <summary>Revokes volume permissions from a principal.</summary>
    public Task RevokeAsync(string volume, string principal, DfsPermissions permissions, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.RevokeAsync: implemented in a later phase");

    /// <summary>Adds a new data-key version for the volume.</summary>
    public Task RotateDataKeyAsync(string volume, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.RotateDataKeyAsync: implemented in a later phase");

    /// <summary>Exports the key-escrow recovery key wrapped under <paramref name="passphrase"/>.</summary>
    public Task<byte[]> ExportRecoveryKeyAsync(string passphrase, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.ExportRecoveryKeyAsync: implemented in a later phase");

    /// <summary>Starts a scrub on one node, or on all nodes when <paramref name="node"/> is null.</summary>
    public Task StartScrubAsync(string? node = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.StartScrubAsync: implemented in a later phase");

    /// <summary>Starts a repair pass for the volume.</summary>
    public Task StartRepairAsync(string volume, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.StartRepairAsync: implemented in a later phase");

    /// <summary>Reads the volume's replica health.</summary>
    public Task<DfsReplicaHealth> GetReplicaHealthAsync(string volume, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GetReplicaHealthAsync: implemented in a later phase");

    /// <summary>Restores a published backup snapshot into a new volume with a new epoch.</summary>
    public Task<DfsVolumeInfo> RestoreFromBackupAsync(DfsRestoreFromBackupRequest request, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.RestoreFromBackupAsync: implemented in a later phase");

    /// <summary>Enables or disables a cluster-wide feature (activates only when every member supports it).</summary>
    public Task SetFeatureAsync(string feature, bool enabled, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.SetFeatureAsync: implemented in a later phase");

    /// <summary>Reads the most recent audit records (never containing payloads or secrets).</summary>
    public Task<IReadOnlyList<string>> GetAuditLogAsync(int max = 1000, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GetAuditLogAsync: implemented in a later phase");
}
