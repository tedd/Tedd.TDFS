namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Feature plug-in points of the node runtime (docs/INTERNALS.md §1.3). Each feature area implements its own partial
/// method in a partial file it owns (for example <c>Volumes/Service/NodeRuntime.Volumes.cs</c>), so areas never edit the
/// same file. Unimplemented hooks compile away.
/// </summary>
internal sealed partial class NodeRuntime
{
    /// <summary>
    /// Registers the volume service's RPC handlers (file, namespace, handle, session, transaction, snapshot and enumeration
    /// requests) with <see cref="NodeServices.Rpc"/>. Runs on the node loop once every service of <see cref="NodeServices"/>
    /// exists (object store open, control group not yet opened); requests are refused until the node serves.
    /// </summary>
    partial void RegisterVolumeHandlers(NodeServices services);

    /// <summary>Registers data-plane handlers beyond <c>ObjectService</c> (replication, inventory, scrub and repair).</summary>
    partial void RegisterDataHandlers(NodeServices services);

    /// <summary>Registers backup handlers (exporter control, catalog queries).</summary>
    partial void RegisterBackupHandlers(NodeServices services);

    /// <summary>Starts node-wide periodic jobs of the volume area through <see cref="NodeServices.Jobs"/> (after serving starts).</summary>
    partial void StartVolumeJobs(NodeServices services);

    /// <summary>Starts node-wide periodic jobs of the data area (inventory reports, scrubbing, repair).</summary>
    partial void StartDataJobs(NodeServices services);

    /// <summary>Starts node-wide periodic jobs of the backup area.</summary>
    partial void StartBackupJobs(NodeServices services);

    private void RegisterFeatureHandlers()
    {
        RegisterVolumeHandlers(Services);
        RegisterDataHandlers(Services);
        RegisterBackupHandlers(Services);
    }

    private void StartFeatureJobs()
    {
        StartVolumeJobs(Services);
        StartDataJobs(Services);
        StartBackupJobs(Services);
    }
}
