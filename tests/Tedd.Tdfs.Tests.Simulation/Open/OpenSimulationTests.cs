using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Open;

/// <summary>
/// OPEN suite, level S: races, interrupted opens, lost replies and inode-vs-path identity of handles under
/// deterministic whole-cluster simulation (FSD §§4 NS-06, 6.1, 7, 13; test plan §7.4).
/// </summary>
public sealed class OpenSimulationTests
{
    private const string Vol = OpenHelpers.Volume;

    private static CancellationToken Ct => OpenHelpers.Ct;

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-002 CreateNewRace
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// 2..10 clients race CreateNew on the same absent (optionally previously deleted) name on 3..10-node clusters:
    /// exactly one logical create wins, every loser gets AlreadyExists, exactly one new inode exists on every replica,
    /// the winner's handle is the only reservation, and the visible content is exactly the winner's bytes.
    /// </summary>
    [Theory, Catalog("OPEN-002", "S", "INV-02", "INV-07")]
    [InlineData(3, 2, false, 0x0201UL)]
    [InlineData(3, 8, false, 0x0202UL)]
    [InlineData(3, 8, true, 0x0203UL)]
    [InlineData(5, 5, false, 0x0204UL)]
    [InlineData(5, 10, true, 0x0205UL)]
    [InlineData(7, 6, false, 0x0206UL)]
    [InlineData(10, 10, true, 0x0207UL)]
    public async Task OPEN_002_CreateNewRace(int nodeCount, int clientCount, bool previouslyDeleted, ulong seed)
    {
        await ClusterTest.RunAsync(OpenHelpers.Deterministic(nodeCount, seed), async cluster =>
        {
            await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            DfsFileClient setupFiles = setup.GetVolume(Vol).Files;
            await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/race", Ct);
            const string path = "/race/f";
            InodeId? deleted = null;
            if (previouslyDeleted)
            {
                await setupFiles.WriteAllBytesAsync(path, TestData.Bytes(seed, 123), Ct);
                deleted = (await setupFiles.GetInfoAsync(path, Ct)).InodeId;
                await setupFiles.DeleteAsync(path, Ct);
            }

            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;
            var clients = new List<DfsClient>();
            for (int i = 0; i < clientCount; i++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "c" + i }, Ct));
            try
            {
                async Task<(DfsStream? Stream, Exception? Error)> AttemptAsync(int i)
                {
                    FileAccess access = i % 2 == 0 ? FileAccess.Write : FileAccess.ReadWrite;
                    FileShare share = (i % 3) switch { 0 => FileShare.None, 1 => FileShare.Read, _ => FileShare.Read | FileShare.Delete };
                    try
                    {
                        return (await clients[i].GetVolume(Vol).Files.OpenAsync(path, FileMode.CreateNew, access, share, Ct), null);
                    }
                    catch (Exception ex)
                    {
                        return (null, ex);
                    }
                }

                (DfsStream? Stream, Exception? Error)[] results = await Task.WhenAll(Enumerable.Range(0, clientCount).Select(AttemptAsync));
                int winnerIndex = Array.FindIndex(results, r => r.Stream is not null);
                Assert.True(winnerIndex >= 0, "no CreateNew won the race on an absent name: " + string.Join("; ", results.Select(r => OpenHelpers.Describe(r.Error))));
                Assert.Single(results, r => r.Stream is not null);
                for (int i = 0; i < results.Length; i++)
                {
                    if (i == winnerIndex)
                        continue;
                    DfsAlreadyExistsException loser = Assert.IsType<DfsAlreadyExistsException>(results[i].Error, exactMatch: false);
                    Assert.Equal(DfsErrorCode.AlreadyExists, loser.ErrorCode);
                }

                DfsStream winner = results[winnerIndex].Stream!;
                DfsFileInfo info = await setupFiles.GetInfoAsync(path, Ct);
                Assert.Equal(winner.InodeId, info.InodeId);
                Assert.Equal(0L, info.Length);
                if (deleted is not null)
                    Assert.NotEqual(deleted.Value, info.InodeId); // no resurrection of the deleted inode

                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                Assert.Equal(inodesBefore + 1, state.InodeCount);
                HandleInspection reservation = Assert.Single(state.Handles, h => h.Inode == winner.InodeId && !h.Expired);
                Assert.True(reservation.IsWriter);
                Assert.Equal(winner.WriterGeneration, reservation.Generation);
                DfsDirectoryPage page = await setup.GetVolume(Vol).Directories.ListPageAsync("/race", null, null, Ct);
                DfsEntryInfo entry = Assert.Single(page.Entries);
                Assert.Equal("f", entry.Name);
                Assert.Equal(winner.InodeId, entry.InodeId);

                byte[] payload = TestData.Bytes(seed * 31 + (ulong)winnerIndex, 10_000);
                await winner.WriteAsync(payload, Ct);
                await winner.DisposeAsync();
                await OpenHelpers.AssertBytesAsync(payload, setupFiles.ReadAllBytesAsync(path, Ct), "the visible file holds exactly the winner's bytes");

                await OpenHelpers.AssertConvergedAsync(cluster);
                foreach ((string node, int count) in await OpenHelpers.InodeCountsAsync(cluster))
                    Assert.True(count == inodesBefore + 1, $"replica {node} has {count} inodes, expected {inodesBefore + 1}: a losing create leaked an inode");
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-007 OpenOrCreateRace
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Concurrent OpenOrCreate calls on an absent name: exactly one inode is created atomically, all admitted handles
    /// refer to it and are pairwise share-compatible with at most one writer, and every rejected caller conflicted with
    /// an admitted handle (typed, retryable sharing violation).
    /// </summary>
    [Theory, Catalog("OPEN-007", "S", "INV-02", "INV-05", "INV-07")]
    [InlineData("writers", 3, 4, 0x0701UL)]
    [InlineData("writers", 5, 8, 0x0702UL)]
    [InlineData("writer-and-sharing-readers", 3, 6, 0x0703UL)]
    [InlineData("writer-and-sharing-readers", 5, 9, 0x0704UL)]
    [InlineData("exclusive-readers-vs-writer", 3, 5, 0x0705UL)]
    [InlineData("exclusive-readers", 3, 4, 0x0706UL)]
    [InlineData("mixed", 3, 8, 0x0707UL)]
    [InlineData("mixed", 5, 10, 0x0708UL)]
    [InlineData("mixed", 7, 10, 0x0709UL)]
    public async Task OPEN_007_OpenOrCreateRace(string scenario, int nodeCount, int clientCount, ulong seed)
    {
        var requests = new (FileAccess Access, FileShare Share)[clientCount];
        var random = new SeededRandom(seed);
        int writerSlot = (int)(seed % (ulong)clientCount);
        for (int i = 0; i < clientCount; i++)
        {
            requests[i] = scenario switch
            {
                "writers" => (FileAccess.Write, FileShare.Read),
                "writer-and-sharing-readers" => i == writerSlot ? (FileAccess.ReadWrite, FileShare.Read) : (FileAccess.Read, FileShare.ReadWrite),
                "exclusive-readers-vs-writer" => i == writerSlot ? (FileAccess.Write, FileShare.Read) : (FileAccess.Read, FileShare.Read),
                "exclusive-readers" => (FileAccess.Read, FileShare.None),
                "mixed" => OpenHelpers.LegalHandles[random.Next(OpenHelpers.LegalHandles.Count)],
                _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
            };
        }

        await ClusterTest.RunAsync(OpenHelpers.Deterministic(nodeCount, seed), async cluster =>
        {
            await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            DfsFileClient setupFiles = setup.GetVolume(Vol).Files;
            await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/ooc", Ct);
            const string path = "/ooc/f";
            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;
            var clients = new List<DfsClient>();
            for (int i = 0; i < clientCount; i++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "c" + i }, Ct));
            var admitted = new List<int>();
            (DfsStream? Stream, Exception? Error)[] results = [];
            try
            {
                async Task<(DfsStream?, Exception?)> AttemptAsync(int i)
                {
                    try
                    {
                        return (await clients[i].GetVolume(Vol).Files.OpenAsync(path, FileMode.OpenOrCreate, requests[i].Access, requests[i].Share, Ct), null);
                    }
                    catch (Exception ex)
                    {
                        return (null, ex);
                    }
                }

                results = await Task.WhenAll(Enumerable.Range(0, clientCount).Select(AttemptAsync));
                for (int i = 0; i < results.Length; i++)
                {
                    OpenHelpers.Log($"c{i} {OpenHelpers.Name(requests[i].Access, requests[i].Share)}: {(results[i].Stream is not null ? "admitted" : OpenHelpers.Describe(results[i].Error))}");
                    if (results[i].Stream is not null)
                        admitted.Add(i);
                }

                Assert.NotEmpty(admitted); // the first evaluated request always creates and is admitted
                InodeId inode = results[admitted[0]].Stream!.InodeId;
                Assert.All(admitted, i => Assert.Equal(inode, results[i].Stream!.InodeId));
                DfsFileInfo info = await setupFiles.GetInfoAsync(path, Ct);
                Assert.Equal(inode, info.InodeId);
                Assert.Equal(0L, info.Length);
                Assert.Equal(inodesBefore + 1, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);

                // Admitted handles are pairwise compatible; at most one writer.
                for (int x = 0; x < admitted.Count; x++)
                {
                    for (int y = x + 1; y < admitted.Count; y++)
                    {
                        (FileAccess a1, FileShare s1) = requests[admitted[x]];
                        (FileAccess a2, FileShare s2) = requests[admitted[y]];
                        Assert.True(OpenHelpers.IsCompatible(a1, s1, a2, s2),
                            $"incompatible handles c{admitted[x]} {OpenHelpers.Name(a1, s1)} and c{admitted[y]} {OpenHelpers.Name(a2, s2)} were both admitted");
                    }
                }

                Assert.True(admitted.Count(i => (requests[i].Access & FileAccess.Write) != 0) <= 1, "more than one writer admitted (LOCK-01)");

                // Every rejection is a typed sharing conflict with some admitted handle (no handle closed during the race).
                for (int i = 0; i < results.Length; i++)
                {
                    if (results[i].Stream is not null)
                        continue;
                    DfsSharingViolationException conflict = Assert.IsType<DfsSharingViolationException>(results[i].Error, exactMatch: false);
                    Assert.Contains(conflict.ErrorCode, new[] { DfsErrorCode.SharingViolation, DfsErrorCode.WriterConflict });
                    Assert.True(conflict.IsRetryable);
                    int rejected = i;
                    Assert.True(admitted.Any(j => !OpenHelpers.IsCompatible(requests[j].Access, requests[j].Share, requests[rejected].Access, requests[rejected].Share)),
                        $"c{i} {OpenHelpers.Name(requests[i].Access, requests[i].Share)} was rejected although it is compatible with every admitted handle");
                }

                // The leader's reservations are exactly the admitted handles.
                IReadOnlyList<HandleInspection> live = await OpenHelpers.LiveHandlesAsync(cluster, inode);
                Assert.Equal(
                    admitted.Select(i => OpenHelpers.Name(requests[i].Access, requests[i].Share)).Order(StringComparer.Ordinal),
                    live.Select(h => OpenHelpers.Name(h.Access, h.Share)).Order(StringComparer.Ordinal));

                switch (scenario)
                {
                    case "writers":
                    case "exclusive-readers":
                        Assert.Single(admitted);
                        break;
                    case "writer-and-sharing-readers":
                        Assert.Equal(clientCount, admitted.Count);
                        break;
                    case "exclusive-readers-vs-writer":
                        Assert.True(admitted.SequenceEqual([writerSlot]) || admitted.SequenceEqual(Enumerable.Range(0, clientCount).Where(i => i != writerSlot)),
                            "either the writer alone or all exclusive readers may be admitted: " + string.Join(",", admitted));
                        break;
                }
            }
            finally
            {
                foreach ((DfsStream? stream, _) in results)
                {
                    if (stream is not null)
                        await stream.DisposeAsync();
                }

                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }

            Assert.True(await setupFiles.ExistsAsync(path, Ct));
            Assert.Empty(await setupFiles.ReadAllBytesAsync(path, Ct));
            Assert.Empty(await OpenHelpers.LiveHandlesAsync(cluster, (await setupFiles.GetInfoAsync(path, Ct)).InodeId));
            await OpenHelpers.AssertConvergedAsync(cluster);
            foreach ((string node, int count) in await OpenHelpers.InodeCountsAsync(cluster))
                Assert.True(count == inodesBefore + 1, $"replica {node} has {count} inodes, expected exactly one new inode");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-014 ReservationAndCreateAreAtomic
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string, string> AtomicityCases
    {
        get
        {
            var data = new TheoryData<string, string, string>();
            (string Site, string Action)[] points =
            [
                (FaultSites.RaftBeforePropose, "block"),
                (FaultSites.RaftAfterLocalAppend, "block"),
                (FaultSites.RaftAfterCommitBeforeApply, "block"),
                (FaultSites.ApplyAfterBeforeReply, "block"),
                (FaultSites.RpcReplySend, "drop-reply"),
                (FaultSites.RaftAfterLocalAppend, "crash-leader"),
                (FaultSites.RaftAfterCommitBeforeApply, "crash-leader"),
                (FaultSites.ApplyAfterBeforeReply, "crash-leader"),
            ];
            foreach (string mode in new[] { "CreateNew-missing", "Create-existing" })
            {
                foreach ((string site, string action) in points)
                    data.Add(mode, site, action);
            }

            return data;
        }
    }

    /// <summary>
    /// A creating (CreateNew) or truncating (Create on an existing file) writer open is suspended, has its reply dropped,
    /// or loses its leader at each server stage F10–F13. At every probe — while suspended, shortly after, and once
    /// resolved — whenever another client or any replica can observe the create/truncate, the writer reservation
    /// (Share.None) exists in the same applied state and a conflicting open is refused. The final outcome is single and
    /// definite (success, or an unknown outcome resolved by operation id; a definite failure was never committed).
    /// </summary>
    [Theory, Catalog("OPEN-014", "S", "INV-02", "INV-05")]
    [MemberData(nameof(AtomicityCases))]
    public async Task OPEN_014_ReservationAndCreateAreAtomic(string mode, string site, string action)
    {
        bool creating = mode == "CreateNew-missing";
        await ClusterTest.RunAsync(OpenHelpers.Deterministic(3, OpenHelpers.SeedOf("OPEN-014", mode, site, action)), async cluster =>
        {
            await using DfsClient target = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient prober = await cluster.ConnectAsync(new TestClientOptions { Name = "probe", OperationTimeout = TimeSpan.FromMilliseconds(500) }, Ct);
            DfsVolume volume = target.GetVolume(Vol);
            DfsFileClient probe = prober.GetVolume(Vol).Files;
            await prober.GetVolume(Vol).Directories.CreateDirectoryAsync("/atom", Ct);
            const string path = "/atom/f";
            byte[] old = TestData.Bytes(0x1401, 6000);
            DfsFileInfo? original = null;
            if (!creating)
            {
                await probe.WriteAllBytesAsync(path, old, Ct);
                original = await probe.GetInfoAsync(path, Ct);
            }

            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);
            var violations = new List<string>();
            long triggeredAt = cluster.Time.GetTimestamp();

            async Task ProbeAsync(string phase)
            {
                bool? visible = null;
                try
                {
                    if (creating)
                        visible = await probe.ExistsAsync(path, Ct);
                    else
                        visible = (await probe.GetInfoAsync(path, Ct)).Revision != original!.Revision;
                }
                catch (Exception ex) when (OpenHelpers.IsIndeterminate(ex))
                {
                }

                OpenHelpers.Log($"{phase}: visible to another client = {visible?.ToString() ?? "indeterminate"}");
                if (visible == true)
                {
                    try
                    {
                        DfsStream intruder = await probe.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                        await intruder.DisposeAsync();
                        // Only a reservation younger than one lease (2 s) is guaranteed to be live without renewal.
                        if (cluster.Time.GetElapsedTime(triggeredAt) < TimeSpan.FromSeconds(1.5))
                            violations.Add($"{phase}: the {mode} effect was visible but a conflicting reader was admitted (unreserved intermediate state)");
                        else
                            OpenHelpers.Log($"{phase}: conflicting reader admitted after the unrenewed reservation could have expired");
                    }
                    catch (DfsSharingViolationException)
                    {
                    }
                    catch (Exception ex) when (OpenHelpers.IsIndeterminate(ex))
                    {
                    }
                    catch (Exception ex)
                    {
                        violations.Add($"{phase}: conflicting probe failed unexpectedly: {OpenHelpers.Describe(ex)}");
                    }
                }

                foreach (TestNode node in cluster.Nodes)
                {
                    if (node.State != TestNodeState.Running)
                        continue;
                    Bounded<VolumeStateInspection?> inspected = await OpenHelpers.WithinAsync(cluster, node.Inspect.GetVolumeStateAsync(Vol), TimeSpan.FromSeconds(1));
                    if (!inspected.Completed || inspected.Result is null)
                        continue; // that node's loop is held at the fault point; checked at the next probe
                    VolumeStateInspection state = inspected.Result;
                    FileInspection? file = state.GetFile(path);
                    bool applied = creating ? file is not null : file is not null && file.Revision != original!.Revision;
                    if (applied && !state.Handles.Any(h => h.Inode == file!.Inode && h.IsWriter && h.Share == FileShare.None))
                        violations.Add($"{phase}: replica {node.Name} applied the {mode} without its writer reservation in the same state");
                }
            }

            FaultAction faultAction = action switch
            {
                "block" => FaultAction.Block(),
                "drop-reply" => FaultAction.DropReply(),
                "crash-leader" => FaultAction.CrashNode(CrashMode.LoseUnflushed),
                _ => throw new ArgumentOutOfRangeException(nameof(action)),
            };
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Volume: Vol, Operation: DfsOperationKind.Open), faultAction);
            Task<DfsStream> open = volume.Files.OpenAsync(path, new DfsOpenOptions
            {
                Mode = creating ? FileMode.CreateNew : FileMode.Create, Access = FileAccess.Write, Share = FileShare.None, OperationId = id,
            }, Ct);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(20));
            cluster.Faults.AssertReached(arm);
            triggeredAt = cluster.Time.GetTimestamp();

            await ProbeAsync($"at {site} ({action})");
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(300), Ct);
            await ProbeAsync($"300 ms after {site} ({action})");
            if (action == "block")
                arm.Release();

            Bounded<DfsStream> resolved = await OpenHelpers.WithinAsync(cluster, open, TimeSpan.FromSeconds(30));
            arm.Disarm();
            Assert.True(resolved.Completed, "the open must resolve within 30 s once the fault point is released or a new leader is elected");
            await ProbeAsync("after the open resolved");

            if (action == "crash-leader")
                await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);

            if (resolved.Result is { } stream)
            {
                DfsFileInfo info = await probe.GetInfoAsync(path, Ct);
                Assert.Equal(stream.InodeId, info.InodeId);
                Assert.Equal(0L, info.Length);
                if (!creating)
                    Assert.Equal(original!.InodeId, info.InodeId);
                HandleInspection reservation = Assert.Single(await OpenHelpers.LiveHandlesAsync(cluster, stream.InodeId), h => h.IsWriter);
                Assert.Equal(FileShare.None, reservation.Share);
                Assert.Equal(stream.WriterGeneration, reservation.Generation);
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(outcome.Token.Sequence, info.Revision); // one effect, nothing re-applied after it
                await Assert.ThrowsAsync<DfsSharingViolationException>(() => probe.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct));
                await stream.DisposeAsync();
                await using DfsStream reader = await probe.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                Assert.Equal(0L, await reader.GetLengthAsync(Ct));
            }
            else
            {
                Exception error = resolved.Error!;
                OpenHelpers.Log("open failed with " + OpenHelpers.Describe(error));
                if (error is DfsCommitOutcomeUnknownException unknown)
                    Assert.Equal(id, unknown.OperationId);
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
                Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Committed, DfsOutcomeStatus.Aborted });
                if (error is not DfsCommitOutcomeUnknownException)
                    Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status); // a definite failure must not have committed

                if (outcome.Status == DfsOutcomeStatus.Committed)
                {
                    DfsFileInfo info = await probe.GetInfoAsync(path, Ct);
                    Assert.Equal(0L, info.Length);
                    if (!creating)
                        Assert.Equal(original!.InodeId, info.InodeId);
                    try
                    {
                        DfsStream early = await probe.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                        await early.DisposeAsync();
                        // Admission is only legal once the unrenewed reservation has expired (LOCK-05).
                        Assert.DoesNotContain(await OpenHelpers.LiveHandlesAsync(cluster, info.InodeId), h => h.IsWriter && h.Share == FileShare.None);
                    }
                    catch (DfsSharingViolationException)
                    {
                    }

                    await cluster.WaitUntilAsync(async () => (await OpenHelpers.LiveHandlesAsync(cluster, info.InodeId)).Count == 0,
                        TimeSpan.FromSeconds(15), "the reservation of an open whose outcome never reached its client is released only by server-side lease expiry (LOCK-05)", Ct);
                    await using DfsStream reader = await probe.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                    Assert.Equal(0L, await reader.GetLengthAsync(Ct));
                }
                else if (creating)
                {
                    Assert.False(await probe.ExistsAsync(path, Ct));
                }
                else
                {
                    await OpenHelpers.AssertBytesAsync(old, probe.ReadAllBytesAsync(path, Ct), "an aborted truncating open leaves the old bytes");
                }
            }

            Assert.Empty(violations);
            await OpenHelpers.AssertConvergedAsync(cluster);
            foreach ((string node, int count) in await OpenHelpers.InodeCountsAsync(cluster))
                Assert.True(count <= inodesBefore + (creating ? 1 : 0), $"replica {node} has {count} inodes: an interrupted open leaked an inode");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-015 OpenReplyLost
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> ReplyLostCases
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (string mode in new[] { "Create-existing", "Truncate-existing", "CreateNew-missing", "OpenOrCreate-missing", "Append-missing" })
            {
                foreach (string retry in new[] { "sdk-retry", "app-retry-same-id", "app-retry-after-expiry" })
                    data.Add(mode, retry);
            }

            return data;
        }
    }

    /// <summary>
    /// A creating/truncating open commits but its reply is lost (F13). Retrying the same operation id — inside the SDK,
    /// by the application after an unknown outcome, or after the never-renewed reservation expired and another client
    /// rewrote the file — returns the original outcome: no second create/truncate (revision equals the original commit
    /// token, foreign data is not truncated), no second independent reservation, never AlreadyExists for its own create.
    /// </summary>
    [Theory, Catalog("OPEN-015", "S", "INV-02", "INV-05", "INV-07")]
    [MemberData(nameof(ReplyLostCases))]
    public async Task OPEN_015_OpenReplyLost(string mode, string retry)
    {
        bool existing = mode.EndsWith("-existing", StringComparison.Ordinal);
        FileMode fileMode = Enum.Parse<FileMode>(mode[..mode.IndexOf('-', StringComparison.Ordinal)]);
        await ClusterTest.RunAsync(OpenHelpers.Deterministic(3, OpenHelpers.SeedOf("OPEN-015", mode, retry)), async cluster =>
        {
            TimeSpan operationTimeout = retry == "sdk-retry" ? TimeSpan.FromSeconds(10) : TimeSpan.FromMilliseconds(500);
            await using DfsClient a = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = operationTimeout }, Ct);
            await using DfsClient b = await cluster.ConnectAsync(new TestClientOptions { Name = "other" }, Ct);
            DfsVolume va = a.GetVolume(Vol);
            DfsFileClient fb = b.GetVolume(Vol).Files;
            const string path = "/f";
            DfsFileInfo? before = null;
            ulong generationBefore = 0;
            if (existing)
            {
                await fb.WriteAllBytesAsync(path, TestData.Bytes(0x1501, 7000), Ct);
                before = await fb.GetInfoAsync(path, Ct);
                generationBefore = Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path)).WriterGeneration;
            }

            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;
            int expectedInodes = inodesBefore + (existing ? 0 : 1);
            DfsOperationId id = await va.NewOperationIdAsync(Ct);
            var options = new DfsOpenOptions { Mode = fileMode, Access = FileAccess.Write, Share = FileShare.Read, OperationId = id };
            byte[] data = TestData.Bytes(0x1502, 9000);
            long requestsBefore = OpenHelpers.CountOpenRequests(cluster, "c0");
            long queriesBefore = cluster.Network.Log.CountMessages(new MessageFilter(From: "client:c0", Type: MessageType.QueryOperation));

            FaultArm dropOpenReplies = cluster.Faults.Arm(
                new FaultMatch(FaultSites.RpcReplySend, Volume: Vol, Operation: DfsOperationKind.Open, Count: retry == "sdk-retry" ? 1 : 1_000_000),
                FaultAction.DropReply());
            FaultArm? dropQueryReplies = retry == "sdk-retry"
                ? null
                : cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Volume: Vol, Operation: DfsOperationKind.QueryOperation, Count: 1_000_000), FaultAction.DropReply());

            if (retry == "sdk-retry")
            {
                DfsStream s = await va.Files.OpenAsync(path, options, Ct);
                cluster.Faults.AssertReached(dropOpenReplies);
                dropOpenReplies.Disarm();
                long retransmissions = OpenHelpers.CountOpenRequests(cluster, "c0") - requestsBefore;
                long queries = cluster.Network.Log.CountMessages(new MessageFilter(From: "client:c0", Type: MessageType.QueryOperation)) - queriesBefore;
                Assert.True(retransmissions >= 2 || queries >= 1,
                    "after the lost reply the SDK must retransmit the open under the same id or query that id; it may not guess the outcome");
                await AssertSingleEffectAsync(s.InodeId, s.WriterGeneration);
                await s.WriteAsync(data, Ct);
                await s.DisposeAsync();
                await OpenHelpers.AssertBytesAsync(data, fb.ReadAllBytesAsync(path, Ct), "the retried open truncated/created exactly once");
                return;
            }

            Exception? first = await Record.ExceptionAsync(() => va.Files.OpenAsync(path, options, Ct));
            cluster.Faults.AssertReached(dropOpenReplies);
            DfsCommitOutcomeUnknownException unknown = Assert.IsType<DfsCommitOutcomeUnknownException>(first, exactMatch: false);
            Assert.Equal(id, unknown.OperationId);
            dropOpenReplies.Disarm();
            dropQueryReplies!.Disarm();

            DfsOperationOutcome outcome = await va.QueryOperationAsync(id, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status); // the leader applied it; only replies were lost
            DfsFileInfo committed = await fb.GetInfoAsync(path, Ct);
            Assert.Equal(0L, committed.Length);
            Assert.Equal(outcome.Token.Sequence, committed.Revision);
            if (existing)
                Assert.Equal(before!.InodeId, committed.InodeId);
            Assert.Equal(expectedInodes, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);

            if (retry == "app-retry-same-id")
            {
                DfsStream s = await va.Files.OpenAsync(path, options, Ct);
                Assert.Equal(committed.InodeId, s.InodeId);
                await AssertSingleEffectAsync(s.InodeId, s.WriterGeneration);
                Assert.Equal(committed.Revision, (await fb.GetInfoAsync(path, Ct)).Revision);
                await s.WriteAsync(data, Ct);
                await s.DisposeAsync();
                await OpenHelpers.AssertBytesAsync(data, fb.ReadAllBytesAsync(path, Ct), "writes through the replayed handle");
                return;
            }

            // app-retry-after-expiry: the reservation created by the lost open is never renewed and expires; another
            // client then rewrites the file. A late retry of the same id must not truncate that foreign data.
            await cluster.WaitUntilAsync(async () => (await OpenHelpers.LiveHandlesAsync(cluster, committed.InodeId)).Count == 0,
                TimeSpan.FromSeconds(15), "the never-renewed reservation of the lost open expires after the 2 s lease (LOCK-05)", Ct);
            byte[] foreign = TestData.Bytes(0x1503, 4321);
            await fb.WriteAllBytesAsync(path, foreign, Ct);
            DfsFileInfo afterForeign = await fb.GetInfoAsync(path, Ct);
            ulong generationAfterForeign = Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path)).WriterGeneration;

            DfsStream? replayed = null;
            Exception? retryError = null;
            try
            {
                replayed = await va.Files.OpenAsync(path, options, Ct);
            }
            catch (Exception ex)
            {
                retryError = ex;
            }

            Assert.False(retryError is DfsAlreadyExistsException, "a retried id must return its original outcome, not be re-evaluated as a new create");
            if (replayed is not null)
            {
                // The replayed outcome is bound to the expired reservation: it can never publish (no regained authority).
                Exception? write = await Record.ExceptionAsync(async () =>
                {
                    await replayed.WriteAsync(TestData.Bytes(0x1504, 10), Ct);
                    await replayed.FlushAsync(Ct);
                });
                Assert.True(write is DfsLeaseLostException or DfsPinExpiredException or DfsException { ErrorCode: DfsErrorCode.LeaseLost or DfsErrorCode.InvalidHandle },
                    "publishing through the replayed, expired reservation must fail with lost authority, got " + OpenHelpers.Describe(write));
                Assert.True(replayed.IsFaulted);
                _ = await Record.ExceptionAsync(() => replayed.DisposeAsync().AsTask());
            }
            else
            {
                Assert.True(retryError is DfsLeaseLostException or DfsOperationExpiredException or DfsPinExpiredException
                        or DfsException { ErrorCode: DfsErrorCode.LeaseLost or DfsErrorCode.InvalidHandle or DfsErrorCode.OperationExpired },
                    "a late retry must report the original (now expired) outcome, got " + OpenHelpers.Describe(retryError));
            }

            DfsFileInfo final = await fb.GetInfoAsync(path, Ct);
            Assert.Equal(afterForeign.Revision, final.Revision);
            await OpenHelpers.AssertBytesAsync(foreign, fb.ReadAllBytesAsync(path, Ct), "a late retry must not truncate data written after its original commit");
            Assert.DoesNotContain(await OpenHelpers.LiveHandlesAsync(cluster, final.InodeId), h => h.IsWriter); // no regained reservation
            Assert.Equal(generationAfterForeign, Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path)).WriterGeneration);
            Assert.Equal(expectedInodes, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);

            async Task AssertSingleEffectAsync(InodeId inode, ulong generation)
            {
                DfsFileInfo info = await fb.GetInfoAsync(path, Ct);
                Assert.Equal(inode, info.InodeId);
                Assert.Equal(0L, info.Length);
                if (existing)
                    Assert.Equal(before!.InodeId, inode);
                FileInspection file = Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path));
                Assert.Equal(generation, file.WriterGeneration);
                if (existing)
                    Assert.True(generation > generationBefore);
                HandleInspection reservation = Assert.Single(await OpenHelpers.LiveHandlesAsync(cluster, inode)); // no second lease
                Assert.True(reservation.IsWriter);
                Assert.Equal(generation, reservation.Generation);
                DfsOperationOutcome o = await va.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, o.Status);
                Assert.Equal(o.Token.Sequence, info.Revision); // no second truncate/create after the original commit
                Assert.Equal(expectedInodes, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-016 RenameDoesNotRetargetHandle
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string, bool> RenameCases
    {
        get
        {
            var data = new TheoryData<string, string, bool>();
            foreach (string kind in new[] { "reader-live", "reader-pinned", "writer-onflush", "writer-writethrough", "append" })
            {
                foreach (string move in new[] { "same-dir", "cross-dir", "parent-dir" })
                {
                    data.Add(kind, move, false);
                    data.Add(kind, move, true);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// Rename an open inode (within a directory, across directories, or by moving its parent), recreate the old name with
    /// different bytes, optionally fail over the volume leader: the existing handle keeps reading/writing the original
    /// inode (now at the new path) and never touches the recreated name.
    /// </summary>
    [Theory, Catalog("OPEN-016", "S", "INV-05", "INV-07")]
    [MemberData(nameof(RenameCases))]
    public async Task OPEN_016_RenameDoesNotRetargetHandle(string kind, string move, bool failover)
    {
        await ClusterTest.RunAsync(OpenHelpers.Deterministic(3, OpenHelpers.SeedOf("OPEN-016", kind, move, failover.ToString())), async cluster =>
        {
            await using DfsClient holder = await cluster.ConnectAsync(new TestClientOptions { Name = "holder" }, Ct);
            await using DfsClient actor = await cluster.ConnectAsync(new TestClientOptions { Name = "actor" }, Ct);
            DfsFileClient hf = holder.GetVolume(Vol).Files;
            DfsFileClient af = actor.GetVolume(Vol).Files;
            DfsDirectoryClient ad = actor.GetVolume(Vol).Directories;
            await ad.CreateDirectoryAsync("/d", Ct);
            await ad.CreateDirectoryAsync("/e", Ct);
            byte[] a0 = TestData.Bytes(0x1601, 6000), w1 = TestData.Bytes(0x1602, 1000), w2 = TestData.Bytes(0x1603, 1500), recreatedBytes = TestData.Bytes(0x1604, 5000);
            await af.WriteAllBytesAsync("/d/a", a0, Ct);
            InodeId original = (await af.GetInfoAsync("/d/a", Ct)).InodeId;

            const FileShare readerShare = FileShare.ReadWrite | FileShare.Delete;
            const FileShare writerShare = FileShare.Read | FileShare.Delete;
            DfsStream h = kind switch
            {
                "reader-live" => await hf.OpenAsync("/d/a", FileMode.Open, FileAccess.Read, readerShare, Ct),
                "reader-pinned" => await hf.OpenAsync("/d/a", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = readerShare, ReadView = DfsReadView.VersionPinned }, Ct),
                "writer-onflush" => await hf.OpenAsync("/d/a", FileMode.Open, FileAccess.ReadWrite, writerShare, Ct),
                "writer-writethrough" => await hf.OpenAsync("/d/a", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = writerShare, WriteMode = DfsWriteMode.WriteThrough }, Ct),
                "append" => await hf.OpenAsync("/d/a", FileMode.Append, FileAccess.Write, writerShare, Ct),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            await using (h)
            {
                Assert.Equal(original, h.InodeId);
                bool writes = h.CanWrite;
                if (writes)
                    await h.WriteAsync(w1, Ct);

                string newPath;
                switch (move)
                {
                    case "same-dir":
                        await af.MoveAsync("/d/a", "/d/b", overwrite: false, Ct);
                        newPath = "/d/b";
                        break;
                    case "cross-dir":
                        await af.MoveAsync("/d/a", "/e/moved", overwrite: false, Ct);
                        newPath = "/e/moved";
                        break;
                    default:
                        await ad.MoveAsync("/d", "/d2", Ct);
                        await ad.CreateDirectoryAsync("/d", Ct);
                        newPath = "/d2/a";
                        break;
                }

                Assert.Equal(original, (await af.GetInfoAsync(newPath, Ct)).InodeId);
                Assert.False(await af.ExistsAsync("/d/a", Ct));
                await af.WriteAllBytesAsync("/d/a", recreatedBytes, Ct);
                InodeId recreated = (await af.GetInfoAsync("/d/a", Ct)).InodeId;
                Assert.NotEqual(original, recreated);

                if (failover)
                    await OpenHelpers.FailOverVolumeLeaderAsync(cluster);

                byte[] expected = kind switch
                {
                    "reader-live" or "reader-pinned" => a0,
                    "append" => OpenHelpers.Concat(a0, w1, w2),
                    _ => OpenHelpers.Overlay(a0, OpenHelpers.Concat(w1, w2)),
                };
                if (writes)
                {
                    await h.WriteAsync(w2, Ct);
                    await h.FlushAsync(Ct);
                }

                if (h.CanRead)
                {
                    h.Position = 0;
                    await OpenHelpers.AssertBytesAsync(expected, OpenHelpers.ReadToEndAsync(h), "the handle still addresses the original inode");
                }

                Assert.Equal(original, h.InodeId);
                Assert.Equal("/d/a", h.Path); // informational path at open time; never re-resolved
                Assert.False(h.IsFaulted);
                await OpenHelpers.AssertBytesAsync(expected, OpenHelpers.ReadSharedAsync(af, newPath), "the renamed inode received the handle's writes");
                await OpenHelpers.AssertBytesAsync(recreatedBytes, af.ReadAllBytesAsync("/d/a", Ct), "the recreated name is untouched by the old handle");
                Assert.Equal(recreated, (await af.GetInfoAsync("/d/a", Ct)).InodeId);
            }

            Assert.Equal(original, (await af.GetInfoAsync(move == "same-dir" ? "/d/b" : move == "cross-dir" ? "/e/moved" : "/d2/a", Ct)).InodeId);
            await OpenHelpers.AssertBytesAsync(recreatedBytes, af.ReadAllBytesAsync("/d/a", Ct), "disposing the old handle does not touch the recreated name");
            await OpenHelpers.AssertConvergedAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-017 UnlinkDoesNotResurrectName
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, bool> UnlinkCases
    {
        get
        {
            var data = new TheoryData<string, bool>();
            foreach (string kind in new[] { "writer-onflush", "writer-writethrough", "append", "reader" })
            {
                data.Add(kind, false);
                data.Add(kind, true);
            }

            return data;
        }
    }

    /// <summary>
    /// Delete a file whose handles all granted FileShare.Delete, optionally fail over the leader, then write/read through
    /// the old handle: bytes go to the detached inode, the deleted path never reappears (not even after flush), a
    /// recreated name is an independent inode the old handle cannot touch, and the detached inode is reclaimed after
    /// the last handle closes.
    /// </summary>
    [Theory, Catalog("OPEN-017", "S", "INV-05", "INV-07")]
    [MemberData(nameof(UnlinkCases))]
    public async Task OPEN_017_UnlinkDoesNotResurrectName(string kind, bool failover)
    {
        await ClusterTest.RunAsync(OpenHelpers.Deterministic(3, OpenHelpers.SeedOf("OPEN-017", kind, failover.ToString())), async cluster =>
        {
            await using DfsClient holder = await cluster.ConnectAsync(new TestClientOptions { Name = "holder" }, Ct);
            await using DfsClient actor = await cluster.ConnectAsync(new TestClientOptions { Name = "actor" }, Ct);
            DfsFileClient hf = holder.GetVolume(Vol).Files;
            DfsFileClient af = actor.GetVolume(Vol).Files;
            DfsDirectoryClient ad = actor.GetVolume(Vol).Directories;
            await ad.CreateDirectoryAsync("/u", Ct);
            byte[] x0 = TestData.Bytes(0x1701, 6000), w1 = TestData.Bytes(0x1702, 800), w2 = TestData.Bytes(0x1703, 900),
                w3 = TestData.Bytes(0x1704, 700), replacement = TestData.Bytes(0x1705, 3333);
            await af.WriteAllBytesAsync("/u/x", x0, Ct);
            InodeId original = (await af.GetInfoAsync("/u/x", Ct)).InodeId;
            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;

            DfsStream h = kind switch
            {
                "writer-onflush" => await hf.OpenAsync("/u/x", FileMode.Open, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, Ct),
                "writer-writethrough" => await hf.OpenAsync("/u/x", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read | FileShare.Delete, WriteMode = DfsWriteMode.WriteThrough }, Ct),
                "append" => await hf.OpenAsync("/u/x", FileMode.Append, FileAccess.Write, FileShare.Read | FileShare.Delete, Ct),
                "reader" => await hf.OpenAsync("/u/x", FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, Ct),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            await using (h)
            {
                bool writes = h.CanWrite;
                if (writes)
                    await h.WriteAsync(w1, Ct);

                CommitToken deletion = await af.DeleteAsync("/u/x", Ct);
                Assert.False(deletion.IsNone);
                await AssertNameAbsentAsync("after delete");
                Assert.Contains(await OpenHelpers.LiveHandlesAsync(cluster, original), r => r.Inode == original); // still attached to the old inode

                if (failover)
                    await OpenHelpers.FailOverVolumeLeaderAsync(cluster);

                byte[] expected = kind switch
                {
                    "reader" => x0,
                    "append" => OpenHelpers.Concat(x0, w1, w2),
                    _ => OpenHelpers.Overlay(x0, OpenHelpers.Concat(w1, w2)),
                };
                if (writes)
                {
                    await h.WriteAsync(w2, Ct);
                    await h.FlushAsync(Ct);
                }

                if (h.CanRead)
                {
                    h.Position = 0;
                    await OpenHelpers.AssertBytesAsync(expected, OpenHelpers.ReadToEndAsync(h), "the detached inode holds the handle's bytes");
                }

                Assert.False(h.IsFaulted);
                await AssertNameAbsentAsync("after writing/flushing through the old handle");

                // Recreate the name: a new inode the old handle cannot reach.
                await using (DfsStream fresh = await af.OpenAsync("/u/x", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct))
                {
                    Assert.NotEqual(original, fresh.InodeId);
                    await fresh.WriteAsync(replacement, Ct);
                }

                if (writes)
                {
                    if (h.CanSeek)
                        h.Position = w1.Length + w2.Length; // the read above moved the position to EOF
                    await h.WriteAsync(w3, Ct);
                    await h.FlushAsync(Ct);
                    expected = kind == "append" ? OpenHelpers.Concat(expected, w3) : OpenHelpers.Overlay(expected, w3, w1.Length + w2.Length);
                }

                if (h.CanRead)
                {
                    h.Position = 0;
                    await OpenHelpers.AssertBytesAsync(expected, OpenHelpers.ReadToEndAsync(h), "the old handle still addresses the detached inode");
                }

                Assert.Equal(original, h.InodeId);
                await OpenHelpers.AssertBytesAsync(replacement, af.ReadAllBytesAsync("/u/x", Ct), "the recreated name is untouched by the old handle");
            }

            DfsFileInfo recreated = await af.GetInfoAsync("/u/x", Ct);
            Assert.NotEqual(original, recreated.InodeId);
            await OpenHelpers.AssertBytesAsync(replacement, af.ReadAllBytesAsync("/u/x", Ct), "closing the old handle does not resurrect or overwrite the name");
            DfsEntryInfo entry = Assert.Single((await ad.ListPageAsync("/u", null, null, Ct)).Entries);
            Assert.Equal(recreated.InodeId, entry.InodeId);
            await cluster.WaitUntilAsync(async () => (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount == inodesBefore,
                TimeSpan.FromSeconds(15), "the detached inode is reclaimed once its last handle closed (NS-06); only the recreated inode remains", Ct);
            await OpenHelpers.AssertConvergedAsync(cluster);

            async Task AssertNameAbsentAsync(string stage)
            {
                Assert.False(await af.ExistsAsync("/u/x", Ct), stage + ": the deleted path is visible again");
                await Assert.ThrowsAsync<FileNotFoundException>(() => af.GetInfoAsync("/u/x", Ct));
                Assert.Empty((await ad.ListPageAsync("/u", null, null, Ct)).Entries);
                Assert.Null(await cluster.Inspect.GetFileAsync(Vol, "/u/x"));
            }
        }, Ct);
    }

    /// <summary>
    /// Negative control for OPEN-017: while any handle lacks FileShare.Delete, delete and rename-over of the inode fail
    /// with a typed sharing violation and leave name, inode and bytes unchanged.
    /// </summary>
    [Theory, Catalog("OPEN-017", "S", "INV-05", "INV-07")]
    [InlineData((int)FileAccess.Read, (int)FileShare.ReadWrite)]
    [InlineData((int)FileAccess.Read, (int)FileShare.None)]
    [InlineData((int)FileAccess.Write, (int)FileShare.Read)]
    [InlineData((int)FileAccess.ReadWrite, (int)FileShare.None)]
    public async Task OPEN_017_UnlinkDoesNotResurrectName_DeleteRequiresShareDelete(int accessValue, int shareValue)
    {
        var access = (FileAccess)accessValue;
        var share = (FileShare)shareValue;
        await ClusterTest.RunAsync(OpenHelpers.Deterministic(3, OpenHelpers.SeedOf("OPEN-017-neg", access.ToString(), share.ToString())), async cluster =>
        {
            await using DfsClient holder = await cluster.ConnectAsync(new TestClientOptions { Name = "holder" }, Ct);
            await using DfsClient actor = await cluster.ConnectAsync(new TestClientOptions { Name = "actor" }, Ct);
            DfsFileClient af = actor.GetVolume(Vol).Files;
            byte[] y = TestData.Bytes(0x1711, 2500), other = TestData.Bytes(0x1712, 100);
            await af.WriteAllBytesAsync("/y", y, Ct);
            await af.WriteAllBytesAsync("/other", other, Ct);
            DfsFileInfo before = await af.GetInfoAsync("/y", Ct);

            DfsStream h = await holder.GetVolume(Vol).Files.OpenAsync("/y", FileMode.Open, access, share, Ct);
            DfsSharingViolationException del = await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(() => af.DeleteAsync("/y", Ct), DfsErrorCode.SharingViolation);
            Assert.True(del.IsRetryable);
            await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(() => af.MoveAsync("/other", "/y", overwrite: true, Ct), DfsErrorCode.SharingViolation);

            DfsFileInfo after = await af.GetInfoAsync("/y", Ct);
            Assert.Equal(before.InodeId, after.InodeId);
            Assert.Equal(before.Revision, after.Revision);
            Assert.Equal(before.InodeId, h.InodeId);
            Assert.False(h.IsFaulted);
            await h.DisposeAsync(); // its share mode may exclude readers; compare bytes once it is closed
            await OpenHelpers.AssertBytesAsync(y, af.ReadAllBytesAsync("/y", Ct), "a refused delete/rename-over changes nothing");
            await OpenHelpers.AssertBytesAsync(other, af.ReadAllBytesAsync("/other", Ct), "the refused rename source is unchanged");
            await af.DeleteAsync("/y", Ct); // allowed once the restrictive handle is closed
            Assert.False(await af.ExistsAsync("/y", Ct));
        }, Ct);
    }
}
