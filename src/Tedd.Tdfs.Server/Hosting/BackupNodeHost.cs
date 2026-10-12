namespace Tedd.Tdfs.Server;

/// <summary>
/// A running late-sync backup node: receives snapshot exports, stores and publishes them, and serves read-only
/// snapshot views. Never a member of any Raft group.
/// </summary>
public sealed class BackupNodeHost : IAsyncDisposable
{
    private BackupNodeHost()
    {
    }

    /// <summary>Backup node name.</summary>
    public string Name => throw new NotImplementedException("BackupNodeHost.Name: implemented in a later phase");

    /// <summary>Actual bound endpoint.</summary>
    public string Endpoint => throw new NotImplementedException("BackupNodeHost.Endpoint: implemented in a later phase");

    /// <summary>Read-only diagnostics.</summary>
    public BackupInspector Inspect => throw new NotImplementedException("BackupNodeHost.Inspect: implemented in a later phase");

    /// <summary>Starts a backup node (fails with <see cref="DfsRecoveryKeyMissingException"/> when key material is missing).</summary>
    public static Task<BackupNodeHost> StartAsync(BackupNodeOptions options, NodeEnvironment environment, CancellationToken ct = default)
        => throw new NotImplementedException("BackupNodeHost.StartAsync: implemented in a later phase");

    /// <summary>Graceful stop.</summary>
    public Task StopAsync(CancellationToken ct = default)
        => throw new NotImplementedException("BackupNodeHost.StopAsync: implemented in a later phase");

    /// <summary>Abrupt stop (crash).</summary>
    public void Kill() => throw new NotImplementedException("BackupNodeHost.Kill: implemented in a later phase");

    /// <summary>Freezes the node loop.</summary>
    public void Pause() => throw new NotImplementedException("BackupNodeHost.Pause: implemented in a later phase");

    /// <summary>Unfreezes the node loop.</summary>
    public void Resume() => throw new NotImplementedException("BackupNodeHost.Resume: implemented in a later phase");

    /// <summary>Stops the node gracefully if still running.</summary>
    public ValueTask DisposeAsync() => throw new NotImplementedException("BackupNodeHost.DisposeAsync: implemented in a later phase");
}
