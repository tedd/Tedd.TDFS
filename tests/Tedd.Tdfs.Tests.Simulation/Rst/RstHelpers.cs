using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Rst;

/// <summary>Shared setup for RST deterministic-simulation scenarios.</summary>
internal static class RstHelpers
{
    public const string Vol = RecoveryScenario.Volume;

    /// <summary>Deterministic source cluster with backup nodes and the fixture's named policy.</summary>
    public static TestClusterOptions SourceOptions(int nodeCount = 3, int backups = 1, TimeSpan? scenarioTimeout = null) => new()
    {
        NodeCount = nodeCount,
        ControlVoters = Math.Min(nodeCount, 3),
        Mode = ClusterMode.Deterministic,
        BackupNodeCount = backups,
        Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = VolumeContentFixture.NamedPolicies }],
        ScenarioTimeout = scenarioTimeout ?? TimeSpan.FromMinutes(10),
    };

    /// <summary>Deterministic recovery cluster (nodes r0..) inside the same simulation.</summary>
    public static TestClusterOptions RecoveryOptions(int nodeCount = 3) => new()
    {
        Nodes = Enumerable.Range(0, nodeCount).Select(i => new TestNodeSpec("r" + i.ToString(CultureInfo.InvariantCulture), "z" + (i + 1).ToString(CultureInfo.InvariantCulture))).ToArray(),
        ControlVoters = Math.Min(nodeCount, 3),
        Mode = ClusterMode.Deterministic,
        Volumes = [],
    };

    /// <summary>Connects to a backup host with the given credentials.</summary>
    public static Task<DfsClient> ConnectBackupAsync(TestCluster cluster, TestBackupNode backup, string name, DfsCredentials credentials, CancellationToken ct) =>
        cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = name, Credentials = credentials }, ct);

    /// <summary>
    /// Asserts a (possibly half-restored) volume is either not served at all — with an availability/not-found error — or
    /// served completely and exactly as <paramref name="expected"/>; a partial namespace or missing data is a failure.
    /// </summary>
    public static async Task AssertNotExposedIncompleteAsync(DfsClient probe, string volume, ExpectedVolumeContent expected, string when, CancellationToken ct)
    {
        ObservedVolumeContent? seen = null;
        Exception? failure = await RecoveryScenario.CaptureAsync(async () => seen = await VolumeContentFixture.CaptureAsync(probe.GetVolume(volume), ct));
        if (failure is null)
        {
            Assert.NotNull(seen);
            VolumeContentFixture.AssertMatches(expected, seen, $"a restored volume that is served {when} must be complete");
        }
        else
        {
            RecoveryScenario.AssertFailedWith(failure, $"an incomplete restored volume must not be served {when}",
                DfsErrorCode.VolumeNotFound, DfsErrorCode.Unavailable, DfsErrorCode.QuorumUnavailable, DfsErrorCode.NotLeader,
                DfsErrorCode.NodeUnavailable, DfsErrorCode.Timeout);
        }
    }

    /// <summary>Asserts the backup host is in no live control/volume group (voter or learner) and not a live cluster member.</summary>
    public static async Task AssertBackupNotLiveMemberAsync(TestCluster cluster, string backupName, string because)
    {
        GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
        Assert.DoesNotContain(backupName, control.Voters);
        Assert.DoesNotContain(backupName, control.Learners);
        GroupInspection volume = await cluster.Inspect.GetVolumeGroupAsync(Vol);
        Assert.DoesNotContain(backupName, volume.Voters);
        Assert.DoesNotContain(backupName, volume.Learners);
        Assert.True(volume.Leader is null || !string.Equals(volume.Leader, backupName, StringComparison.Ordinal), $"backup {backupName} leads the volume group ({because})");
        Assert.True(control.Leader is null || !string.Equals(control.Leader, backupName, StringComparison.Ordinal), $"backup {backupName} leads the control group ({because})");
    }

    /// <summary>
    /// Attempts every mutation family through a backup-host client and asserts each is rejected with
    /// <see cref="DfsReadOnlyException"/>; also asserts the facades advertise read-only.
    /// </summary>
    public static async Task AssertBackupReadOnlyAsync(DfsClient client, string because, CancellationToken ct)
    {
        DfsVolume v = client.GetVolume(Vol);
        Assert.True(v.Files.IsReadOnly, $"backup file facade must be read-only ({because})");
        Assert.True(v.Directories.IsReadOnly, $"backup directory facade must be read-only ({because})");
        Assert.True(v.Snapshots.IsReadOnly, $"backup snapshot facade must be read-only ({because})");
        var attempts = new (string Name, Func<Task> Action)[]
        {
            ("WriteAllBytes", () => v.Files.WriteAllBytesAsync("/promoted.bin", new byte[] { 1 }, ct)),
            ("AppendAllBytes", () => v.Files.AppendAllBytesAsync("/docs/one.bin", new byte[] { 1 }, ct)),
            ("Create", async () => { await using DfsStream s = await v.Files.CreateAsync("/promoted-create.bin", ct); }),
            ("OpenReadWrite", async () => { await using DfsStream s = await v.Files.OpenAsync("/docs/one.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, ct); }),
            ("OpenTruncate", async () => { await using DfsStream s = await v.Files.OpenAsync("/docs/one.bin", FileMode.Truncate, FileAccess.Write, FileShare.None, ct); }),
            ("Delete", () => v.Files.DeleteAsync("/docs/one.bin", ct)),
            ("Move", () => v.Files.MoveAsync("/docs/one.bin", "/moved.bin", false, ct)),
            ("Copy", () => v.Files.CopyAsync("/docs/one.bin", "/copied.bin", false, ct)),
            ("SetAccess", () => v.Files.SetAccessAsync("/docs/one.bin", new DfsInodeAccess(OwnerOnly: true), ct)),
            ("SetStoragePolicy", () => v.Files.SetStoragePolicyAsync("/docs/one.bin", VolumeContentFixture.PairPolicy, ct)),
            ("CreateDirectory", () => v.Directories.CreateDirectoryAsync("/promoted-dir", ct)),
            ("DeleteDirectory", () => v.Directories.DeleteAsync("/empty", false, ct)),
            ("MoveDirectory", () => v.Directories.MoveAsync("/docs", "/docs-moved", ct)),
            ("Transaction", async () =>
            {
                await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
                await tx.Files.WriteAllBytesAsync("/tx-promoted.bin", new byte[] { 1 }, ct);
                await tx.CommitAsync(ct);
            }),
            ("CreateSnapshot", () => v.Snapshots.CreateAsync("promoted-snapshot", null, ct)),
        };
        foreach ((string name, Func<Task> action) in attempts)
        {
            Exception? e = await RecoveryScenario.CaptureAsync(action);
            Assert.True(e is DfsReadOnlyException,
                $"{name} through the backup host must be rejected with DfsReadOnlyException but got {(e is null ? "success" : e.GetType().Name + ": " + e.Message)} ({because})");
        }
    }
}
