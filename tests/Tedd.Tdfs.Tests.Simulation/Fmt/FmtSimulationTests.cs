using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Simulation.Ops;

namespace Tedd.Tdfs.Tests.Simulation.Fmt;

/// <summary>
/// FMT suite, level S: rolling upgrades, interrupted migrations and feature activation across the supported release
/// window (FSD §§3.2, 15, 17; test plan §7.25), using <see cref="SoftwareVersionProfile"/> to run nodes as specific
/// releases under deterministic simulation.
/// </summary>
public sealed class FmtSimulationTests
{
    private const string Vol = FmtHelpers.Volume;

    private static CancellationToken Ct => FmtHelpers.Ct;

    // ============================================================================================================
    // FMT-007 RollingUpgradeMixedVersions
    // ============================================================================================================

    /// <summary>
    /// The whole cluster starts on the oldest supported release; then, for every newer release of the supported window in
    /// turn, nodes are upgraded one at a time (followers first, leader last) under continuous register/append load. In every
    /// mixed state the active feature set is understood by every running release, a newly connecting client negotiates a
    /// protocol version inside the members' advertised ranges and is never offered a feature an older member cannot
    /// interpret, and there is never more than one leader per term; quorum is retained throughout (writes keep completing
    /// while each node is down). After the last upgrade the new-only feature can be activated, the full history is
    /// linearizable and replicas converge.
    /// </summary>
    [Theory, Catalog("FMT-007", "S", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData(3)]
    [InlineData(5)]
    public async Task FMT_007_RollingUpgradeMixedVersions(int nodes)
    {
        IReadOnlyList<SoftwareVersionProfile> window = SoftwareVersionProfile.SupportedWindow;
        SoftwareVersionProfile oldest = window[0];
        SoftwareVersionProfile current = SoftwareVersionProfile.Current;
        Assert.Equal(SoftwareVersionProfile.Previous.Name, oldest.Name);
        Assert.Equal(current.Name, window[^1].Name);
        Assert.True(window.Count >= 2, "a rolling upgrade needs at least two releases in the supported window");
        await VersionedClusterTest.RunAsync(FmtHelpers.Deterministic(nodes), oldest, async cluster =>
        {
            FmtHelpers.Log($"window: {string.Join(", ", window)}");
            Assert.All(cluster.Nodes, n => Assert.Equal(oldest.Name, n.SoftwareVersion.Name));

            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            await using DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "c1", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            await using DfsClient r0 = await cluster.ConnectAsync(new TestClientOptions { Name = "r0", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            Assert.InRange(c0.Capabilities.ProtocolVersion, oldest.ProtocolVersionMin, oldest.ProtocolVersionMax);
            Assert.DoesNotContain(SoftwareVersionProfile.ProbeFeature, (await cluster.Admin.GetClusterStatusAsync(Ct)).ActiveFeatures);

            await c0.GetVolume(Vol).Directories.CreateDirectoryAsync("/fmt007", Ct);
            var workload = new HistoryWorkload(cluster, "/fmt007");
            workload.Start([(c0, "k0"), (c1, "k1"), (c0, "log0")], r0, valueBytes: 4097, pause: TimeSpan.FromMilliseconds(50));

            foreach (SoftwareVersionProfile release in window.Skip(1))
            {
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                IEnumerable<TestNode> order = cluster.Nodes.Where(n => n.Name != leader.Name).Append(leader).ToArray();
                foreach (TestNode node in order)
                {
                    await node.StopAsync();
                    int okWhileDown = workload.OkWrites;
                    await cluster.WaitUntilAsync(() => Task.FromResult(workload.OkWrites > okWhileDown + 2), TimeSpan.FromSeconds(30),
                        $"writes must keep completing while {node.Name} is down for its upgrade to {release.Name} (quorum retained)", Ct);

                    await node.RestartAsSoftwareVersionAsync(release);
                    await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
                    Assert.Equal(release.Name, node.SoftwareVersion.Name);
                    await cluster.Inspect.AssertNoSplitBrainAsync();

                    DfsClusterStatus status = await cluster.Admin.GetClusterStatusAsync(Ct);
                    foreach (string feature in status.ActiveFeatures)
                    {
                        foreach (TestNode member in cluster.Nodes)
                            Assert.True(member.SoftwareVersion.Supports(feature), $"active feature {feature} is not understood by {member.Name} ({member.SoftwareVersion})");
                    }

                    // Protocol versions are negotiated per connection (the highest version both ends speak); features are
                    // cluster-wide and may only be offered when every member understands them.
                    int lowestMin = cluster.Nodes.Min(n => n.SoftwareVersion.ProtocolVersionMin);
                    int highestMax = cluster.Nodes.Max(n => n.SoftwareVersion.ProtocolVersionMax);
                    await using (DfsClient probe = await cluster.ConnectAsync(new TestClientOptions { Name = $"probe-{release.Name}-{node.Name}" }, Ct))
                    {
                        Assert.InRange(probe.Capabilities.ProtocolVersion, lowestMin, highestMax);
                        foreach (string feature in probe.Capabilities.Features)
                        {
                            foreach (TestNode member in cluster.Nodes)
                                Assert.True(member.SoftwareVersion.Supports(feature), $"a client was offered feature {feature} that {member.Name} ({member.SoftwareVersion}) cannot interpret");
                        }

                        if (cluster.Nodes.Any(n => !n.SoftwareVersion.Supports(SoftwareVersionProfile.ProbeFeature)))
                            Assert.DoesNotContain(SoftwareVersionProfile.ProbeFeature, probe.Capabilities.Features);
                    }

                    int okAfter = workload.OkWrites;
                    await cluster.WaitUntilAsync(() => Task.FromResult(workload.OkWrites > okAfter + 2), TimeSpan.FromSeconds(30),
                        $"writes must keep completing after {node.Name} rejoined on {release.Name}", Ct);
                }

                Assert.All(cluster.Nodes, n => Assert.Equal(release.Name, n.SoftwareVersion.Name));
            }

            await cluster.Admin.SetFeatureAsync(SoftwareVersionProfile.ProbeFeature, true, Ct);
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetClusterStatusAsync(Ct)).ActiveFeatures.Contains(SoftwareVersionProfile.ProbeFeature),
                TimeSpan.FromSeconds(30), "once every member runs the new release the new feature can be activated", Ct);
            await using (DfsClient upgraded = await cluster.ConnectAsync(new TestClientOptions { Name = "after" }, Ct))
            {
                Assert.Equal(current.ProtocolVersionMax, upgraded.Capabilities.ProtocolVersion);
                Assert.Contains(SoftwareVersionProfile.ProbeFeature, upgraded.Capabilities.Features);
            }

            await workload.StopAsync();
            await workload.ResolveUnknownsAsync(r0);
            await workload.FinalReadsAsync(r0, ["k0", "k1", "log0"]);
            workload.AssertLinearizable();
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);
    }

    // ============================================================================================================
    // FMT-008 UpgradeInterruptedByCrash
    // ============================================================================================================

    public static TheoryData<string, CrashMode> UpgradeCrashPoints
    {
        get
        {
            var data = new TheoryData<string, CrashMode>();
            foreach (string site in new[] { FaultSites.RecoveryJournalReplay, FaultSites.RecoveryCheckpointInstall, FaultSites.RaftSnapshotInstall, FaultSites.Reconfiguration })
            {
                foreach (CrashMode mode in new[] { CrashMode.LoseUnflushed, CrashMode.TearLastWrite })
                    data.Add(site, mode);
            }

            return data;
        }
    }

    /// <summary>
    /// A follower stopped on the old release (with a local checkpoint, and with the leader's log compacted past it) is
    /// restarted on the new release and crashed at a recovery/migration boundary (journal replay, checkpoint install,
    /// snapshot install, membership/protocol transition). The crash point must be reached; the remaining majority keeps
    /// serving exact data meanwhile; the next start recovers to a documented state — its image is either still the old
    /// format or the new one, never in between — and holds exactly the authoritative replicated state. Rolling the node back
    /// to the old release is then either still possible (old-readable image) or refused with Unsupported without
    /// touching a single byte of the image.
    /// </summary>
    [Theory, Catalog("FMT-008", "S", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(UpgradeCrashPoints))]
    public async Task FMT_008_UpgradeInterruptedByCrash(string site, CrashMode mode)
    {
        TestClusterOptions options = FmtHelpers.Deterministic(3) with { Timings = NodeTimings.Fast with { RaftSnapshotThreshold = 50 } };
        SoftwareVersionProfile previous = SoftwareVersionProfile.Previous;
        SoftwareVersionProfile current = SoftwareVersionProfile.Current;
        await VersionedClusterTest.RunAsync(options, previous, async cluster =>
        {
            Assert.All(cluster.Nodes, n => Assert.Equal(previous.Name, n.SoftwareVersion.Name));

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/fmt008", Ct);
            var expected = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int round = 0; round < 10; round++)
            {
                for (int i = 0; i < 25; i++)
                {
                    string path = $"/fmt008/f{i}";
                    byte[] content = TestData.Bytes((ulong)(round * 100 + i), i % 2 == 0 ? 6000 : 70_000);
                    await vol.Files.WriteAllBytesAsync(path, content, Ct);
                    expected[path] = content;
                }
            }

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode target = cluster.Nodes.First(n => n.Name != leader.Name);
            await target.StopAsync();
            for (int i = 0; i < 120; i++)
            {
                string path = $"/fmt008/f{i % 25}";
                byte[] content = TestData.Bytes((ulong)(5000 + i), 6000);
                await vol.Files.WriteAllBytesAsync(path, content, Ct);
                expected[path] = content;
            }

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: target.Name), FaultAction.CrashNode(mode));
            Exception? interrupted = await Record.ExceptionAsync(() => target.RestartAsSoftwareVersionAsync(current));
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(arm);
            await cluster.WaitUntilAsync(() => Task.FromResult(target.State == TestNodeState.Crashed), TimeSpan.FromSeconds(30),
                $"the fault at {site} must crash the upgrading node", Ct);
            FmtHelpers.Log($"upgrade of {target.Name} crashed at {site} ({mode}); restart call reported {interrupted?.GetType().Name ?? "completion"}");

            await FmtHelpers.VerifyFilesAsync(vol, expected, "served by the majority while the upgrade is interrupted");
            await cluster.Inspect.AssertNoSplitBrainAsync();
            arm.Disarm();

            await target.RestartAsSoftwareVersionAsync(current);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            DiskFormatInspection format = await target.Inspect.GetDiskFormatAsync();
            FmtHelpers.Log($"{target.Name} format after interrupted upgrade: {format}");
            Assert.Contains(format.SuperblockVersion, new[] { previous.DiskFormatVersion, current.DiskFormatVersion });
            Assert.InRange(format.HighestComponentVersion, Math.Min(previous.DiskFormatVersion, current.DiskFormatVersion),
                Math.Max(previous.DiskFormatVersion, current.DiskFormatVersion));
            Assert.True(format.MinimumReaderVersion <= current.MaxReadableDiskFormatVersion);

            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            VolumeStateInspection authoritative = await cluster.Inspect.GetVolumeStateAsync(Vol);
            VolumeStateInspection recovered = await cluster.Inspect.GetVolumeStateAsync(Vol, target.Name);
            foreach (string path in expected.Keys)
                Assert.Equal(authoritative.GetFile(path)!.Revision, recovered.GetFile(path)!.Revision);
            await FmtHelpers.VerifyFilesAsync(vol, expected, "after recovery from the interrupted upgrade");

            // The rollback decision must match the image as durably recorded immediately before the node stops (the recovered
            // node may have written further structures, e.g. a new checkpoint, since the first inspection).
            format = await target.Inspect.GetDiskFormatAsync();
            await target.StopAsync();
            SortedDictionary<string, string> imageBefore = FmtHelpers.Fingerprint(target.Disk);
            Exception? rollback = await Record.ExceptionAsync(() => target.RestartAsSoftwareVersionAsync(previous));
            if (FmtHelpers.CanOpen(previous, format))
            {
                Assert.Null(rollback);
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                await target.StopAsync();
            }
            else
            {
                DfsException refusal = Assert.IsAssignableFrom<DfsException>(rollback);
                Assert.Equal(DfsErrorCode.Unsupported, refusal.ErrorCode);
                Assert.Equal(TestNodeState.Stopped, target.State);
                Assert.Equal(imageBefore, FmtHelpers.Fingerprint(target.Disk));
            }

            await target.RestartAsSoftwareVersionAsync(current);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await FmtHelpers.VerifyFilesAsync(vol, expected, "after the rollback attempt");
        }, Ct);
    }

    // ============================================================================================================
    // FMT-009 FeatureActivationWaitsForEligibility
    // ============================================================================================================

    /// <summary>
    /// While one voter runs a release that does not understand the probe feature, activating it is refused explicitly or
    /// stays inactive (never active, never advertised to clients) and the old member keeps its vote. After that member is
    /// upgraded the feature activates and is advertised. A member then rolled back to the old release cannot silently
    /// rejoin: it is refused at start, or it runs visibly excluded — never leader, never applying state committed under the
    /// feature — while the upgraded majority keeps committing; the feature is never silently deactivated.
    /// </summary>
    [Fact, Catalog("FMT-009", "S", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public async Task FMT_009_FeatureActivationWaitsForEligibility()
    {
        await VersionedClusterTest.RunAsync(FmtHelpers.Deterministic(3), SoftwareVersionProfile.Previous, async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            string feature = SoftwareVersionProfile.ProbeFeature;
            Assert.True(SoftwareVersionProfile.Current.Supports(feature));
            Assert.False(SoftwareVersionProfile.Previous.Supports(feature));

            // Partially upgraded cluster: n0 and n1 move to the current release, n2 stays on the previous one.
            foreach (TestNode upgraded in cluster.Nodes.Take(2))
            {
                await upgraded.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Current);
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            }

            TestNode old = cluster.Nodes[2];
            Assert.Equal(SoftwareVersionProfile.Previous.Name, old.SoftwareVersion.Name);

            Exception? early = await Record.ExceptionAsync(() => admin.SetFeatureAsync(feature, true, Ct));
            if (early is not null)
            {
                // Unsupported/InvalidArgument surface as NotSupportedException/ArgumentException (API.md A.2 canonical mapping,
                // pinned by FMT-011), hence DfsErrors.ToCode rather than a DfsException type check.
                Assert.Contains(DfsErrors.ToCode(early), new[] { DfsErrorCode.Unsupported, DfsErrorCode.InvalidArgument });
            }

            for (int i = 0; i < 10; i++)
            {
                DfsClusterStatus status = await admin.GetClusterStatusAsync(Ct);
                Assert.DoesNotContain(feature, status.ActiveFeatures);
                Assert.Contains(old.Name, status.Control.Voters);
                DfsVolumeStatus volumeStatus = await admin.GetVolumeStatusAsync(Vol, Ct);
                Assert.Contains(old.Name, volumeStatus.Group.Voters);
                Assert.True(volumeStatus.Group.HasQuorum);
                await using (DfsClient probe = await cluster.ConnectAsync(new TestClientOptions { Name = $"early{i}" }, Ct))
                    Assert.DoesNotContain(feature, probe.Capabilities.Features);
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            }

            await old.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Current);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await admin.SetFeatureAsync(feature, true, Ct);
            await cluster.WaitUntilAsync(async () => (await admin.GetClusterStatusAsync(Ct)).ActiveFeatures.Contains(feature),
                TimeSpan.FromSeconds(30), "with every member eligible the feature must activate", Ct);
            await using (DfsClient probe = await cluster.ConnectAsync(new TestClientOptions { Name = "eligible" }, Ct))
                Assert.Contains(feature, probe.Capabilities.Features);

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/fmt009", Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode rolledBack = cluster.Nodes.First(n => n.Name != leader.Name);
            await rolledBack.StopAsync();
            Exception? rejoin = await Record.ExceptionAsync(() => rolledBack.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Previous));
            var expected = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            if (rejoin is not null)
            {
                DfsException refusal = Assert.IsAssignableFrom<DfsException>(rejoin);
                Assert.Equal(DfsErrorCode.Unsupported, refusal.ErrorCode);
                Assert.NotEqual(TestNodeState.Running, rolledBack.State);
            }
            else if (rolledBack.State == TestNodeState.Running)
            {
                // Let the old release finish replaying its own durable log before taking the baseline: only entries it
                // receives from the upgraded majority afterwards would be state committed under the feature.
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                ulong appliedAtStart = (await rolledBack.Inspect.GetGroupAsync(Vol))?.AppliedIndex ?? 0;
                for (int i = 0; i < 10; i++)
                {
                    string path = $"/fmt009/f{i}";
                    expected[path] = TestData.Bytes(900 + (ulong)i, 5000);
                    await vol.Files.WriteAllBytesAsync(path, expected[path], Ct);
                    await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);

                    DfsNodeInfo info = (await admin.ListNodesAsync(Ct)).Single(n => n.Name == rolledBack.Name);
                    Assert.False(info.Status == DfsNodeStatus.Active && info.IsReachable,
                        $"{rolledBack.Name} runs a release that cannot interpret active feature {feature} and must be visibly excluded");
                    Assert.NotEqual(rolledBack.Name, (await admin.GetVolumeStatusAsync(Vol, Ct)).Group.Leader);
                    Assert.Equal(appliedAtStart, (await rolledBack.Inspect.GetGroupAsync(Vol))?.AppliedIndex ?? 0);
                }
            }
            else
            {
                // The old release started and then shut itself down (refused registration): an explicit, visible exclusion
                // too, as long as it never led the group.
                Assert.NotEqual(rolledBack.Name, (await admin.GetVolumeStatusAsync(Vol, Ct)).Group.Leader);
                FmtHelpers.Log($"{rolledBack.Name} on {SoftwareVersionProfile.Previous.Name} stopped by itself after start ({rolledBack.State})");
            }

            Assert.Contains(feature, (await admin.GetClusterStatusAsync(Ct)).ActiveFeatures);
            await vol.Files.WriteAllBytesAsync("/fmt009/majority", TestData.Bytes(990, 5000), Ct);

            if (rolledBack.State == TestNodeState.Running)
                await rolledBack.StopAsync();
            await rolledBack.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Current);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await FmtHelpers.VerifyFilesAsync(vol, expected, "after the rolled-back member returned on the new release");
            Assert.Contains(feature, (await admin.GetClusterStatusAsync(Ct)).ActiveFeatures);
        }, Ct);
    }
}
