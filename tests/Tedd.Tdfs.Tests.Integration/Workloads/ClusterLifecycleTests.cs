using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Integration.Str.StrHelpers;

namespace Tedd.Tdfs.Tests.Integration.Workloads;

public sealed class ClusterLifecycleTests
{
    private static TestClusterOptions Hosts => RealBoundaries() with
    {
        OutOfProcess = true,
        ScenarioTimeout = TimeSpan.FromMinutes(4),
        Timings = NodeTimings.Fast with { LeaseDuration = TimeSpan.FromSeconds(30), SessionTimeout = TimeSpan.FromMinutes(2) },
    };

    private static Task<DfsStream> OpenWriterAsync(DfsClient client, string path) =>
        client.GetVolume(Vol).Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct);

    [Theory]
    [InlineData("/ledger", true)]
    [InlineData("/ledger:book", true)]
    [InlineData("/ledger", false)]
    public Task OneWriterAcrossProcessLossAndRestart(string path, bool killLeader) =>
        ClusterTest.RunAsync(Hosts, async cluster =>
        {
            await using DfsClient observer = await ConnectAsync(cluster, "observer");
            DfsVolume volume = observer.GetVolume(Vol);
            byte[] initial = TestData.Bytes(3101, 65537);
            byte[] staged = TestData.Bytes(3102, 131075);
            await volume.Files.WriteAllBytesAsync("/ledger", initial, Ct);
            if (path != "/ledger")
                await volume.Files.WriteAllBytesAsync(path, initial, Ct);

            var clients = new List<DfsClient>();
            try
            {
                foreach (TestNode node in cluster.Nodes)
                {
                    Assert.Null(node.Host);
                    DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "writer-" + node.Name, SeedNodes = [node.Name] }, Ct);
                    clients.Add(client);
                    await client.GetVolume(Vol).NewOperationIdAsync(Ct);
                }

                var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                async Task<DfsStream?> ContendAsync(DfsClient client)
                {
                    await start.Task;
                    try
                    {
                        return await OpenWriterAsync(client, path);
                    }
                    catch (DfsSharingViolationException ex) when (ex.ErrorCode is DfsErrorCode.WriterConflict or DfsErrorCode.SharingViolation)
                    {
                        return null;
                    }
                }
                Task<DfsStream?>[] contenders = clients.Select(ContendAsync).ToArray();
                start.SetResult();
                DfsStream?[] grants = await Task.WhenAll(contenders);
                DfsStream winner = Assert.Single(grants.OfType<DfsStream>());
                DfsClient challenger = clients[Array.IndexOf(grants, null)];
                ulong generation = winner.WriterGeneration;
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                TestNode victim = killLeader ? leader : cluster.Nodes.First(node => node != leader);
                await using (winner)
                {
                    await winner.WriteAsync(staged, Ct);
                    await victim.CrashAsync();
                    Assert.NotEqual(victim.Name, (await cluster.GetLeaderAsync(Vol, Ct)).Name);
                    await Assert.ThrowsAsync<DfsSharingViolationException>(() => OpenWriterAsync(challenger, path));
                    BytesEqual(initial, await ReadCommittedAsync(volume, path), "losing opens and staging changed no committed bytes");
                    Assert.False((await winner.CommitAsync(Ct)).IsNone);
                    BytesEqual(staged, await ReadCommittedAsync(volume, path), "winner published through the surviving quorum");
                }

                byte[] suffix = TestData.Bytes(3103, 79);
                CommitToken lastCommit;
                await using (DfsStream successor = await OpenWriterAsync(challenger, path))
                {
                    Assert.True(successor.WriterGeneration > generation);
                    successor.Seek(0, SeekOrigin.End);
                    await successor.WriteAsync(suffix, Ct);
                    lastCommit = await successor.CommitAsync(Ct);
                }
                await victim.RestartAsync();
                await cluster.WaitForStableAsync(ct: Ct);
                await cluster.WaitForReplicationAsync(Vol, Ct);
                GroupInspection? recoveredGroup = await victim.InspectGroupAsync(Vol);
                Assert.NotNull(recoveredGroup);
                Assert.True(recoveredGroup.AppliedIndex >= lastCommit.Sequence);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                await using DfsClient returned = await cluster.ConnectAsync(new TestClientOptions { Name = "after", SeedNodes = [victim.Name] }, Ct);
                byte[] expected = [.. staged, .. suffix];
                BytesEqual(expected, await returned.GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct), "read after the failed process caught up");
                if (path != "/ledger")
                    BytesEqual(initial, await volume.Files.ReadAllBytesAsync("/ledger", Ct), "independent default stream");
            }
            finally
            {
                foreach (DfsClient client in clients)
                    await client.DisposeAsync();
            }
        }, Ct);

    [Fact]
    public Task ExpiredWriterCannotPublishAfterLeaderReplacement() =>
        ClusterTest.RunAsync(Hosts with { Timings = Hosts.Timings with { LeaseDuration = TimeSpan.FromSeconds(4) } }, async cluster =>
        {
            const string path = "/fenced";
            var clock = new TestClock(cluster.Time);
            await using DfsClient suspended = await cluster.ConnectAsync(new TestClientOptions { Name = "suspended", Clock = clock }, Ct);
            await using DfsClient current = await ConnectAsync(cluster, "current");
            DfsVolume volume = current.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync(path, TestData.Bytes(3401, 7000), Ct);
            DfsStream stale = await OpenWriterAsync(suspended, path);
            try
            {
                await stale.WriteAsync(TestData.Bytes(3402, 7000), Ct);
                clock.Freeze(); // Retain the old handle and its staged bytes while its keep-alives stop.
                TestNode formerLeader = await cluster.GetLeaderAsync(Vol, Ct);
                await formerLeader.CrashAsync();
                Assert.NotEqual(formerLeader.Name, (await cluster.GetLeaderAsync(Vol, Ct)).Name);

                DfsStream? acquired = null;
                await cluster.WaitUntilAsync(async () =>
                {
                    try
                    {
                        acquired = await OpenWriterAsync(current, path);
                        return true;
                    }
                    catch (DfsSharingViolationException)
                    {
                        return false;
                    }
                }, TimeSpan.FromSeconds(40), "the new leader grants a writer after the old lease expires", Ct);

                await using DfsStream successor = acquired!;
                Assert.True(successor.WriterGeneration > stale.WriterGeneration);
                byte[] authoritative = TestData.Bytes(3403, 14001);
                await successor.WriteAsync(authoritative, Ct);
                CommitToken published = await successor.CommitAsync(Ct);
                await formerLeader.RestartAsync();
                await cluster.WaitForStableAsync(ct: Ct);
                clock.Unfreeze();
                DfsLeaseLostException rejected = await Assert.ThrowsAsync<DfsLeaseLostException>(() => stale.CommitAsync(Ct));
                Assert.Equal(DfsErrorCode.LeaseLost, rejected.ErrorCode);
                Assert.True(stale.IsFaulted);
                BytesEqual(authoritative, await ReadCommittedAsync(volume, path), "rejected stale publication");
                await cluster.WaitForReplicationAsync(Vol, Ct);
                foreach (TestNode node in cluster.Nodes)
                {
                    FileInspection? local = await node.InspectFileAsync(Vol, path);
                    Assert.NotNull(local);
                    Assert.Equal((published.Sequence, successor.WriterGeneration), (local.Revision, local.WriterGeneration));
                }
            }
            finally
            {
                clock.Unfreeze();
                try { await stale.DisposeAsync(); }
                catch (DfsLeaseLostException) when (stale.IsFaulted) { }
            }
        }, Ct);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public Task LiveReplacementMovesVotesAndVerifiedSnapshotCopies(bool drainLeader) =>
        ClusterTest.RunAsync(Hosts, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "operator");
            DfsVolume volume = client.GetVolume(Vol);
            const string path = "/record";
            byte[] initial = TestData.Bytes(3201, 2 * 65536 + 1);
            byte[] current = TestData.Bytes(3202, 65537);
            await volume.Files.WriteAllBytesAsync(path, initial, Ct);
            ObjectId[] originalObjects = await ObjectsAsync();
            string[] originalMembers = cluster.Nodes.Select(node => node.Name).ToArray();
            await WaitForCopiesAsync(originalObjects, originalMembers);
            await volume.Snapshots.CreateAsync("original", ct: Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode retiring = drainLeader ? leader : cluster.Nodes.First(node => node != leader);
            TestNode added = await cluster.AddNodeAsync(new TestNodeSpec("replacement", "z4"), join: true, Ct);
            Assert.Null(added.Host);
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.ListNodesAsync(Ct)).Any(
                n => n.Id == added.Id && n.Status == DfsNodeStatus.Active && n.IsReachable),
                TimeSpan.FromSeconds(30), "the replacement is registered and reporting as reachable", Ct);
            await cluster.Admin.DrainNodeAsync(retiring.Name, Ct);
            await volume.Files.WriteAllBytesAsync(path, current, Ct);
            await cluster.WaitUntilAsync(async () =>
                (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Id == retiring.Id).Status == DfsNodeStatus.Drained,
                TimeSpan.FromMinutes(2), "node evacuated its votes and data", Ct);
            string[] survivors = cluster.Nodes.Where(node => node != retiring).Select(node => node.Name).Order(StringComparer.Ordinal).ToArray();
            Assert.Equal(survivors, (await cluster.Inspect.GetControlGroupAsync()).Voters.Order(StringComparer.Ordinal));
            Assert.Equal(survivors, (await cluster.Inspect.GetVolumeGroupAsync(Vol)).Voters.Order(StringComparer.Ordinal));
            await WaitForCopiesAsync(originalObjects.Concat(await ObjectsAsync()).Distinct().ToArray(), survivors);

            await cluster.Admin.RemoveNodeAsync(retiring.Name, force: false, Ct);
            Assert.DoesNotContain(await cluster.Admin.ListNodesAsync(Ct), n => n.Id == retiring.Id && n.Status != DfsNodeStatus.Removed);
            await retiring.CrashAsync();
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "replacement-reader", SeedNodes = [added.Name] }, Ct);
            DfsVolume after = reader.GetVolume(Vol);
            await using DfsSnapshotView snapshot = await after.Snapshots.OpenAsync("original", Ct);
            BytesEqual(initial, await snapshot.Files.ReadAllBytesAsync(path, Ct), "snapshot after removal");
            BytesEqual(current, await after.Files.ReadAllBytesAsync(path, Ct), "live file after removal");
            byte[] suffix = TestData.Bytes(3203, 17);
            await after.Files.AppendAllBytesAsync(path, suffix, Ct);
            byte[] appended = [.. current, .. suffix];
            BytesEqual(appended, await volume.Files.ReadAllBytesAsync(path, Ct), "new quorum acknowledges further writes");
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);

            async Task<ObjectId[]> ObjectsAsync()
            {
                FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
                Assert.NotNull(file);
                Assert.NotEmpty(file.Extents);
                return file.Extents.Select(e => e.Object).Distinct().ToArray();
            }

            async Task WaitForCopiesAsync(ObjectId[] objects, string[] required)
            {
                await cluster.WaitUntilAsync(async () =>
                {
                    foreach (ObjectId id in objects)
                    {
                        IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                        if (required.Except(holders, StringComparer.Ordinal).Any())
                            return false;
                    }
                    return true;
                }, TimeSpan.FromMinutes(1), $"verified object copies on {string.Join(", ", required)}", Ct);
            }
        }, Ct);

    [Fact]
    public Task SafeRemovalRefusesSoleSnapshotHolderOnlineAndOffline() =>
        ClusterTest.RunAsync(Hosts with
        {
            Volumes = [new TestVolumeSpec(Vol)
            {
                NamedPolicies = new Dictionary<string, DfsStoragePolicy>
                {
                    ["single"] = new() { TargetCopies = 1, TargetDistinctZones = 1, DurableCopiesBeforeAck = 1, DurableZonesBeforeAck = 1 },
                },
            }],
        }, async cluster =>
        {
            const string path = "/retained";
            byte[] payload = TestData.Bytes(3501, 40 * 1024);
            await using DfsClient client = await ConnectAsync(cluster, "operator");
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync(path, payload, new DfsMutationOptions { StoragePolicy = "single" }, Ct);
            FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
            Assert.NotNull(file);
            ObjectId id = Assert.Single(file.Extents).Object;
            TestNode holder = cluster.Node(Assert.Single(await cluster.Inspect.GetObjectHoldersAsync(id)));
            await volume.Snapshots.CreateAsync("retained", ct: Ct);
            await volume.Files.DeleteAsync(path, Ct);

            await AssertRefusedAsync();
            await holder.CrashAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            await AssertRefusedAsync();
            await using (DfsSnapshotView offline = await volume.Snapshots.OpenAsync("retained", Ct))
            {
                Exception? error = await Record.ExceptionAsync(() => offline.Files.ReadAllBytesAsync(path, Ct));
                Assert.True(error is DfsUnavailableException or DfsIntegrityException, $"Missing sole copy returned {error?.ToString() ?? "success"}");
            }
            byte[] next = TestData.Bytes(3502, 4097);
            await volume.Files.WriteAllBytesAsync(path, next, Ct);
            await holder.RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            Assert.Equal(holder.Name, Assert.Single(await cluster.Inspect.GetObjectHoldersAsync(id)));
            await using DfsSnapshotView snapshot = await volume.Snapshots.OpenAsync("retained", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    BytesEqual(payload, await snapshot.Files.ReadAllBytesAsync(path, Ct), "snapshot-only copy after refused removal and restart");
                    return true;
                }
                catch (DfsUnavailableException)
                {
                    return false; // The metadata quorum can recover before the client's data connection reconnects.
                }
            }, TimeSpan.FromSeconds(40), "the original client can read the verified copy on the returned host", Ct);
            BytesEqual(next, await volume.Files.ReadAllBytesAsync(path, Ct), "independent file created during the outage");

            async Task AssertRefusedAsync()
            {
                DfsException refusal = await AssertEx.ThrowsDfsAsync<DfsException>(() => cluster.Admin.RemoveNodeAsync(holder.Name, force: false, Ct));
                Assert.Contains(refusal.ErrorCode, new[] { DfsErrorCode.PlacementUnavailable, DfsErrorCode.QuorumUnavailable,
                    DfsErrorCode.InvalidArgument, DfsErrorCode.InvalidPolicy });
                Assert.Contains(await cluster.Admin.ListNodesAsync(Ct), n => n.Id == holder.Id && n.Status != DfsNodeStatus.Removed);
            }
        }, Ct);
}
