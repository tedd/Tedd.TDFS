using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>A late-sync backup node of a <see cref="TestCluster"/>.</summary>
public sealed partial class TestBackupNode
{
    internal TestBackupNode(string name, int index)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Name = name;
        Index = index;
    }

    /// <summary>Node name ("b0", ...).</summary>
    public string Name { get; }

    /// <summary>Creation index.</summary>
    public int Index { get; }

    /// <summary>Bound endpoint.</summary>
    public string Endpoint => throw Stub.NotYet("TestBackupNode.Endpoint");

    /// <summary>Lifecycle state.</summary>
    public TestNodeState State => throw Stub.NotYet("TestBackupNode.State");

    /// <summary>This node's disk.</summary>
    public TestDisk Disk => throw Stub.NotYet("TestBackupNode.Disk");

    /// <summary>This node's clock.</summary>
    public TestClock Clock => throw Stub.NotYet("TestBackupNode.Clock");

    /// <summary>Read-only diagnostics.</summary>
    public BackupInspector Inspect => throw Stub.NotYet("TestBackupNode.Inspect");

    /// <summary>Registration name used with <see cref="DfsBackupClient"/> ("b0", ...).</summary>
    public string TargetName => throw Stub.NotYet("TestBackupNode.TargetName");

    /// <summary>Export principal credentials (no retention rights).</summary>
    public DfsCredentials ExportCredentials => throw Stub.NotYet("TestBackupNode.ExportCredentials");

    /// <summary>Reader principal credentials.</summary>
    public DfsCredentials ReaderCredentials => throw Stub.NotYet("TestBackupNode.ReaderCredentials");

    /// <summary>Retention-admin principal credentials.</summary>
    public DfsCredentials RetentionAdminCredentials => throw Stub.NotYet("TestBackupNode.RetentionAdminCredentials");

    /// <summary>Crashes the node abruptly.</summary>
    public Task CrashAsync(CrashMode mode = CrashMode.LoseUnflushed) => throw Stub.NotYet("TestBackupNode.CrashAsync");

    /// <summary>Stops the node gracefully.</summary>
    public Task StopAsync() => throw Stub.NotYet("TestBackupNode.StopAsync");

    /// <summary>Restarts the node from its durable image.</summary>
    public Task RestartAsync() => throw Stub.NotYet("TestBackupNode.RestartAsync");

    /// <summary>Freezes the node loop.</summary>
    public Task PauseAsync() => throw Stub.NotYet("TestBackupNode.PauseAsync");

    /// <summary>Unfreezes the node loop.</summary>
    public Task ResumeAsync() => throw Stub.NotYet("TestBackupNode.ResumeAsync");

    /// <summary>Permanently destroys the disk.</summary>
    public Task DestroyDiskAsync() => throw Stub.NotYet("TestBackupNode.DestroyDiskAsync");

    /// <summary>Removes the backup recovery key material (RST-005).</summary>
    public Task RemoveRecoveryKeyAsync() => throw Stub.NotYet("TestBackupNode.RemoveRecoveryKeyAsync");

    /// <summary>Registers this target on a live volume.</summary>
    public Task RegisterAsync(string volume, DfsBackupSchedule? schedule = null, DfsBackupBudgets? budgets = null) =>
        throw Stub.NotYet("TestBackupNode.RegisterAsync");

    /// <inheritdoc />
    public override string ToString() => Name;
}
