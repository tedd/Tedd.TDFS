using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Simulation.Sec;

/// <summary>
/// SEC — authentication, authorization and confidentiality (FSD §§4, 7, 12, 14). Deterministic simulation: revocation
/// during staging, capability expiry/replay, key rotation under I/O and wall-clock changes.
/// </summary>
public sealed class SecSimulationTests
{
    private const string Vol = SecHelpers.Vol;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-006
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>The staged mutation whose publication follows a revocation in SEC-006.</summary>
    public enum StagedMutation
    {
        /// <summary>OnFlush stream: staged (partly uploaded) bytes published by Flush.</summary>
        StreamFlush,

        /// <summary>WriteThrough stream: one multi-chunk write publishes.</summary>
        WriteThroughWrite,

        /// <summary>WriteAllBytes helper (upload then commit).</summary>
        WriteAllBytes,

        /// <summary>AppendAllBytes helper (upload then commit).</summary>
        AppendAllBytes,

        /// <summary>Transaction with a bound stream's staged bytes, then commit.</summary>
        TransactionStreamCommit,

        /// <summary>Transaction with staged move/delete/create-directory, then commit.</summary>
        TransactionNamespaceCommit,

        /// <summary>Transaction commit-and-snapshot after the Snapshot permission is revoked.</summary>
        TransactionCommitWithSnapshot,
    }

    /// <summary>When the permission is revoked relative to the publication in SEC-006.</summary>
    public enum RevokePoint
    {
        /// <summary>After all staging completed, before publication starts.</summary>
        AfterStaging,

        /// <summary>F03: while the client is uploading the second chunk.</summary>
        DuringUpload,

        /// <summary>F04: after the leader accepted the publication request.</summary>
        RequestAccepted,

        /// <summary>F10: after the leader's pre-proposal validation, before proposing.</summary>
        BeforeProposal,
    }

    /// <summary>Valid (mutation, revoke point) pairs.</summary>
    public static TheoryData<StagedMutation, RevokePoint> RevocationCases
    {
        get
        {
            var data = new TheoryData<StagedMutation, RevokePoint>();
            foreach (StagedMutation m in Enum.GetValues<StagedMutation>())
            {
                foreach (RevokePoint p in Enum.GetValues<RevokePoint>())
                {
                    bool uploads = m is StagedMutation.StreamFlush or StagedMutation.WriteThroughWrite or StagedMutation.WriteAllBytes
                        or StagedMutation.AppendAllBytes or StagedMutation.TransactionStreamCommit;
                    bool separateStaging = m is StagedMutation.StreamFlush or StagedMutation.TransactionStreamCommit
                        or StagedMutation.TransactionNamespaceCommit or StagedMutation.TransactionCommitWithSnapshot;
                    if (p == RevokePoint.DuringUpload && !uploads)
                        continue;
                    if (p == RevokePoint.AfterStaging && !separateStaging)
                        continue;
                    data.Add(m, p);
                }
            }

            return data;
        }
    }

    private static DfsOperationKind PublicationKind(StagedMutation m) => m switch
    {
        StagedMutation.StreamFlush or StagedMutation.WriteThroughWrite => DfsOperationKind.CommitFile,
        StagedMutation.WriteAllBytes => DfsOperationKind.WriteAll,
        StagedMutation.AppendAllBytes => DfsOperationKind.Append,
        _ => DfsOperationKind.TxCommit,
    };

    /// <summary>
    /// Stages a mutation as a writer, revokes the needed permission at each point up to the authoritative decision, and
    /// asserts the publication is rejected with AccessDenied, leaves files, revisions and snapshots unchanged, and that the
    /// same principal can publish again once re-granted (positive control).
    /// </summary>
    [Theory]
    [MemberData(nameof(RevocationCases))]
    [Catalog("SEC-006", "S", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_006_PermissionRevokedDuringStaging(StagedMutation mutation, RevokePoint point) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            const string principalName = "stager";
            const string target = "/sec6/target.bin";
            TestPrincipal writer = await cluster.CreatePrincipalAsync(principalName,
                new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write | DfsPermissions.Snapshot });
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            await adminVol.Directories.CreateDirectoryAsync("/sec6", Ct);
            await adminVol.Files.WriteAllBytesAsync(target, TestData.Bytes(601, 2 * chunk + 3), Ct);
            await adminVol.Files.WriteAllTextAsync("/sec6/move-me.txt", "m", null, Ct);
            await adminVol.Files.WriteAllTextAsync("/sec6/delete-me.txt", "d", null, Ct);
            string before = await SecHelpers.SubtreeDigestAsync(adminVol, "/sec6", Ct);
            string[] snapshotsBefore = await SecHelpers.SnapshotNamesAsync(adminVol, Ct);

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = principalName, Credentials = writer.Credentials }, Ct);
            DfsVolume v = client.GetVolume(Vol);
            DfsPermissions revoked = mutation == StagedMutation.TransactionCommitWithSnapshot ? DfsPermissions.Snapshot : DfsPermissions.Write;
            byte[] staged = TestData.Bytes(602, 3 * chunk + 11);
            var cleanup = new List<Func<ValueTask>>();

            async Task<Func<Task>> StageAsync()
            {
                switch (mutation)
                {
                    case StagedMutation.StreamFlush:
                    {
                        DfsStream s = await v.Files.OpenAsync(target, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                        cleanup.Add(s.DisposeAsync);
                        await s.WriteAsync(staged, Ct);
                        Assert.True(s.PendingBytes >= staged.Length, "the bytes are staged, not published");
                        return () => s.FlushAsync(Ct);
                    }

                    case StagedMutation.WriteThroughWrite:
                    {
                        DfsStream s = await v.Files.OpenAsync(target, new DfsOpenOptions
                        {
                            Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough,
                        }, Ct);
                        cleanup.Add(s.DisposeAsync);
                        return () => s.WriteAsync(staged, Ct).AsTask();
                    }

                    case StagedMutation.WriteAllBytes:
                        return () => v.Files.WriteAllBytesAsync(target, staged, Ct);
                    case StagedMutation.AppendAllBytes:
                        return () => v.Files.AppendAllBytesAsync(target, staged, Ct);
                    default:
                    {
                        DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                        cleanup.Add(tx.DisposeAsync);
                        switch (mutation)
                        {
                            case StagedMutation.TransactionStreamCommit:
                                await using (DfsStream s = await tx.Files.OpenAsync(target, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct))
                                    await s.WriteAsync(staged, Ct);
                                return () => tx.CommitAsync(Ct);
                            case StagedMutation.TransactionNamespaceCommit:
                                await tx.Files.MoveAsync("/sec6/move-me.txt", "/sec6/moved.txt", false, Ct);
                                await tx.Files.DeleteAsync("/sec6/delete-me.txt", Ct);
                                await tx.Directories.CreateDirectoryAsync("/sec6/tx-dir", Ct);
                                return () => tx.CommitAsync(Ct);
                            default:
                                await tx.Files.WriteAllBytesAsync(target, staged, Ct);
                                return () => tx.CommitWithSnapshotAsync("sec6-snapshot", Ct);
                        }
                    }
                }
            }

            FaultArm? arm = point switch
            {
                RevokePoint.DuringUpload => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, PathPrefix: "/sec6/", Occurrence: 2), FaultAction.Block()),
                RevokePoint.RequestAccepted => cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Volume: Vol, Operation: PublicationKind(mutation)), FaultAction.Block()),
                RevokePoint.BeforeProposal => cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Volume: Vol, Operation: PublicationKind(mutation)), FaultAction.Block()),
                _ => null,
            };

            Task publication;
            if (arm is null)
            {
                Func<Task> publish = await StageAsync();
                await cluster.Admin.RevokeAsync(Vol, principalName, revoked, Ct);
                publication = publish();
            }
            else
            {
                async Task StageAndPublishAsync()
                {
                    Func<Task> publish = await StageAsync();
                    await publish();
                }

                publication = StageAndPublishAsync();
                await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
                cluster.Faults.AssertReached(arm);
                await cluster.Admin.RevokeAsync(Vol, principalName, revoked, Ct);
                arm.Release();
            }

            Exception? outcome = await RecoveryScenario.AwaitOutcomeAsync(cluster, publication, TimeSpan.FromSeconds(60), $"{mutation} publication after revocation", Ct);
            SecHelpers.AssertAccessDenied(outcome, $"{mutation} published after revoking {revoked} at {point}");
            foreach (Func<ValueTask> dispose in cleanup)
                await RecoveryScenario.CaptureAsync(async () => await dispose());

            Assert.Equal(before, await SecHelpers.SubtreeDigestAsync(adminVol, "/sec6", Ct));
            Assert.Equal(snapshotsBefore, await SecHelpers.SnapshotNamesAsync(adminVol, Ct));

            await cluster.Admin.GrantAsync(Vol, principalName, revoked, Ct);
            byte[] after = TestData.Bytes(603, 1000);
            await cluster.WaitUntilAsync(async () => await RecoveryScenario.CaptureAsync(() => v.Files.WriteAllBytesAsync("/sec6/regranted.bin", after, Ct)) is null,
                TimeSpan.FromSeconds(30), "the re-granted principal can publish again");
            if (revoked == DfsPermissions.Snapshot)
                await v.Snapshots.CreateAsync("sec6-regranted", null, Ct);
            await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, "/sec6/regranted.bin", after, "positive control after re-grant", Ct);
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-007
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Capability (mis)use replayed in SEC-007.</summary>
    public enum CapabilityReplay
    {
        /// <summary>The owner replays a read capability after its handle closed, its lifetime passed and the storage node restarted.</summary>
        ExpiredReadCapabilityAfterRestart,

        /// <summary>Another principal (rights on another volume only) presents the owner's read capability.</summary>
        StolenReadCapability,

        /// <summary>
        /// The file is overwritten and the owner's connection reset; the owner replays its still-valid read capability on a
        /// new connection, and its pinned handle must keep reading the now-historical revision (valid historical access).
        /// </summary>
        ValidReadCapabilityAfterReconnect,

        /// <summary>The owner replays a staging upload after aborting the staging session.</summary>
        StagingCapabilityAfterAbort,

        /// <summary>Another principal presents the owner's staging upload.</summary>
        StolenStagingCapability,
    }

    /// <summary>
    /// Captures real data-plane requests carrying capabilities and replays them after reconnect, expiry, restart (new
    /// incarnation and leader) or from a different principal: misuse is denied, valid same-principal access within the
    /// capability's lifetime is preserved, and nothing replayed is ever published.
    /// </summary>
    [Theory]
    [InlineData(CapabilityReplay.ExpiredReadCapabilityAfterRestart)]
    [InlineData(CapabilityReplay.StolenReadCapability)]
    [InlineData(CapabilityReplay.ValidReadCapabilityAfterReconnect)]
    [InlineData(CapabilityReplay.StagingCapabilityAfterAbort)]
    [InlineData(CapabilityReplay.StolenStagingCapability)]
    [Catalog("SEC-007", "S", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_007_CapabilityExpiryAndReplay(CapabilityReplay replay) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            const string path = "/sec7/data.bin";
            TestPrincipal owner = await cluster.CreatePrincipalAsync("cap-owner", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write });
            TestPrincipal thief = await cluster.CreatePrincipalAsync("cap-thief", new Dictionary<string, DfsPermissions> { [SecHelpers.Vol2] = DfsPermissions.All });
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            byte[] content = TestData.Bytes(701, 3 * chunk + 5);
            await adminVol.Directories.CreateDirectoryAsync("/sec7", Ct);
            await adminVol.Files.WriteAllBytesAsync(path, content, Ct);
            string before = await SecHelpers.SubtreeDigestAsync(adminVol, "/sec7", Ct);
            byte[] newer = TestData.Bytes(703, 2 * chunk + 9);
            byte[] expectedLive = content;

            await using DfsClient p = await cluster.ConnectAsync(new TestClientOptions { Name = "p", Credentials = owner.Credentials }, Ct);
            DfsVolume pv = p.GetVolume(Vol);
            bool readCase = replay is CapabilityReplay.ExpiredReadCapabilityAfterRestart or CapabilityReplay.StolenReadCapability or CapabilityReplay.ValidReadCapabilityAfterReconnect;
            CapturedMessage captured;
            DfsStream? reader = null;
            DfsStream? stagingWriter = null;
            try
            {
                if (readCase)
                {
                    using MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:p", Type: MessageType.GetObject, Direction: LinkDirection.Request));
                    // Share.ReadWrite admits a later writer (test plan §5): the pinned handle, not a sharing lock, protects its revision.
                    reader = await pv.Files.OpenAsync(path, new DfsOpenOptions { ReadView = DfsReadView.VersionPinned, Share = FileShare.ReadWrite }, Ct);
                    var first = new byte[chunk];
                    Task<int> read = reader.ReadAsync(first, Ct).AsTask();
                    await hold.WaitForAsync(1, TimeSpan.FromSeconds(30));
                    captured = hold.Captured[0];
                    hold.ReleaseAll();
                    int n = await read;
                    Assert.True(n > 0);
                    AssertEx.BytesEqual(content.AsSpan(0, n), first.AsSpan(0, n), "the captured read itself was served correctly");
                }
                else
                {
                    using MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:p", Type: MessageType.PutObject, Direction: LinkDirection.Request));
                    stagingWriter = await pv.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                    Task write = stagingWriter.WriteAsync(TestData.Bytes(702, 3 * chunk), Ct).AsTask();
                    await hold.WaitForAsync(1, TimeSpan.FromSeconds(30));
                    captured = hold.Captured[0];
                    hold.ReleaseAll();
                    await write;
                }

                Assert.Equal(LinkDirection.Request, captured.Direction);
                DfsCredentials presenter = replay is CapabilityReplay.StolenReadCapability or CapabilityReplay.StolenStagingCapability ? thief.Credentials : owner.Credentials;
                switch (replay)
                {
                    case CapabilityReplay.ExpiredReadCapabilityAfterRestart:
                    {
                        await reader!.DisposeAsync();
                        reader = null;
                        NodeTimings timings = cluster.Options.Timings;
                        await cluster.DelayAsync(timings.PinDuration * 3 + timings.LeaseDuration * 3, Ct);
                        TestNode storage = cluster.Node(captured.To);
                        await storage.StopAsync();
                        await storage.RestartAsync();
                        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
                        break;
                    }

                    case CapabilityReplay.ValidReadCapabilityAfterReconnect:
                        // The live file moves on (new revision) and the connection is reset: the pinned handle's revision is
                        // now historical, and access to it remains valid for this handle's lifetime.
                        await adminVol.Files.WriteAllBytesAsync(path, newer, Ct);
                        expectedLive = newer;
                        before = await SecHelpers.SubtreeDigestAsync(adminVol, "/sec7", Ct);
                        cluster.Network.ResetConnections("client:p");
                        break;
                    case CapabilityReplay.StagingCapabilityAfterAbort:
                        await stagingWriter!.AbortAsync(Ct);
                        break;
                }

                long putsBefore = (await cluster.Inspect.GetTotalCountersAsync()).ObjectPuts;
                await using RawProtocolClient raw = await cluster.ConnectRawAsync(captured.To, Ct);
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(presenter));
                DfsErrorCode code = await raw.SendFrameAsync(captured.Frame);
                switch (replay)
                {
                    case CapabilityReplay.ExpiredReadCapabilityAfterRestart:
                        Assert.True(code is DfsErrorCode.AccessDenied or DfsErrorCode.PinExpired, $"an expired read capability was honoured after restart: {code}");
                        break;
                    case CapabilityReplay.StolenReadCapability:
                    case CapabilityReplay.StolenStagingCapability:
                        Assert.True(code == DfsErrorCode.AccessDenied, $"a capability presented by another principal must be AccessDenied, got {code}");
                        Assert.Equal(putsBefore, (await cluster.Inspect.GetTotalCountersAsync()).ObjectPuts);
                        break;
                    case CapabilityReplay.ValidReadCapabilityAfterReconnect:
                        // The FSD does not fix whether a capability is bound to the principal only (served on a new connection)
                        // or also to the issuing connection/session (refused): both are safe. What must be preserved is the
                        // valid historical access itself — the pinned handle keeps reading the same revision after reconnect.
                        Assert.True(code is DfsErrorCode.None or DfsErrorCode.AccessDenied or DfsErrorCode.PinExpired,
                            $"a same-principal replay of a still-valid capability may be served or refused, not {code}");
                        TestContext.Current.TestOutputHelper?.WriteLine($"same-principal capability replay after reconnect: {code}");
                        var rest = new byte[content.Length];
                        reader!.Seek(0, SeekOrigin.Begin);
                        int total = 0;
                        while (total < rest.Length)
                        {
                            int n = await reader.ReadAsync(rest.AsMemory(total), Ct);
                            Assert.True(n > 0);
                            total += n;
                        }

                        AssertEx.BytesEqual(content, rest, "the pinned handle keeps reading its (now historical) revision after reconnect");
                        Assert.Equal(content.LongLength, await reader.GetLengthAsync(Ct));
                        break;
                    default:
                        Assert.True(code is DfsErrorCode.None or DfsErrorCode.AccessDenied or DfsErrorCode.InvalidHandle or DfsErrorCode.LeaseLost or DfsErrorCode.PinExpired,
                            $"an upload replayed after abort may only be refused or stored as an unreachable orphan, got {code}");
                        break;
                }

                if (stagingWriter is not null && replay == CapabilityReplay.StolenStagingCapability)
                    await stagingWriter.AbortAsync(Ct);
            }
            finally
            {
                if (reader is not null)
                    await RecoveryScenario.CaptureAsync(async () => await reader.DisposeAsync());
                if (stagingWriter is not null)
                    await RecoveryScenario.CaptureAsync(async () => await stagingWriter.DisposeAsync());
            }

            Assert.Equal(before, await SecHelpers.SubtreeDigestAsync(adminVol, "/sec7", Ct));
            await RecoveryScenario.AssertFileBytesAsync(pv.Files, path, expectedLive, "the owner's legitimate access is unaffected", Ct);
            await using DfsClient q = await cluster.ConnectAsync(new TestClientOptions { Name = "q", Credentials = thief.Credentials }, Ct);
            SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => q.GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct)), "the thief has no namespace access");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-013
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Rotates the volume data key repeatedly (optionally crashing the node executing the rotation) while concurrent
    /// writers and readers run and a replica is crashed and repaired: the read/write history is linearizable per file,
    /// objects of several key versions coexist, every object and a pre-rotation snapshot stay decodable, and scrub finds
    /// no checksum failure.
    /// </summary>
    [Theory]
    [InlineData(1, false)]
    [InlineData(3, false)]
    [InlineData(2, true)]
    [Catalog("SEC-013", "S", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_013_KeyRotationDuringIo(int rotations, bool crashDuringRotation) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            const int files = 4;
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            var initial = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            await adminVol.Directories.CreateDirectoryAsync("/sec13", Ct);
            for (int i = 0; i < files; i++)
            {
                string key = "/sec13/f" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + ".bin";
                initial[key] = TestData.Bytes(1300 + (ulong)i, 2 * chunk + i);
                await adminVol.Files.WriteAllBytesAsync(key, initial[key], Ct);
            }

            await adminVol.Snapshots.CreateAsync("pre-rotation", null, Ct);
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "w" }, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct);
            HistoryRecorder history = cluster.History;
            bool stop = false;
            string[] keys = initial.Keys.ToArray();

            // Failures a correct system may legitimately report while a node crashes or leadership moves. Definite failures
            // (writer conflict with a lingering implicit reservation, lease loss, expired pin) have no effect and are
            // recorded as Failed; unknown outcomes are recorded as Unknown by the history recorder.
            static bool Tolerated(Exception ex) =>
                ex is DfsUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException
                    or DfsSharingViolationException or DfsLeaseLostException or DfsPinExpiredException
                || DfsErrors.ToCode(ex) is DfsErrorCode.NotLeader or DfsErrorCode.Timeout or DfsErrorCode.NodeUnavailable or DfsErrorCode.OutcomeUnknown;

            async Task WriterAsync()
            {
                for (int i = 0; !stop && i < 400; i++)
                {
                    string key = keys[i % files];
                    byte[] data = TestData.Bytes(1400 + (ulong)i, chunk + 17 * i);
                    try
                    {
                        await history.RecordAsync("w", "write", key, data, () => writerClient.GetVolume(Vol).Files.WriteAllBytesAsync(key, data, Ct));
                    }
                    catch (Exception ex) when (Tolerated(ex))
                    {
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(40), Ct);
                }
            }

            async Task ReaderAsync()
            {
                for (int i = 0; !stop && i < 800; i++)
                {
                    string key = keys[(i * 3) % files];
                    try
                    {
                        await history.RecordAsync("r", "read", key, null, () => readerClient.GetVolume(Vol).Files.ReadAllBytesAsync(key, Ct));
                    }
                    catch (Exception ex) when (Tolerated(ex))
                    {
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(25), Ct);
                }
            }

            Task writer = WriterAsync();
            Task reader = ReaderAsync();
            for (int r = 1; r <= rotations; r++)
            {
                bool crash = crashDuringRotation && r == 1;
                FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.KeyRotation, Volume: Vol),
                    crash ? FaultAction.CrashNode() : FaultAction.Delay(TimeSpan.FromMilliseconds(300)));
                Exception? rotation = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RotateDataKeyAsync(Vol, Ct));
                cluster.Faults.AssertReached(arm);
                if (crash)
                {
                    TestNode crashed = cluster.Node(arm.Contexts[0].Node);
                    await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                    await crashed.RestartAsync();
                    await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
                    for (int attempt = 0; rotation is not null && attempt < 5; attempt++)
                    {
                        Assert.True(Tolerated(rotation), $"rotation interrupted by a crash must fail retryably, got {rotation}");
                        rotation = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RotateDataKeyAsync(Vol, Ct));
                    }
                }

                Assert.Null(rotation);
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            }

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode follower = cluster.Nodes.First(n => n.Name != leader.Name);
            await follower.CrashAsync();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            await cluster.Admin.RotateDataKeyAsync(Vol, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            await follower.RestartAsync();
            stop = true;
            await writer;
            await reader;
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(120), Ct);

            foreach (string key in keys)
            {
                HistoryOp[] ops = history.Operations.Where(o => o.Key == key).ToArray();
                Assert.Contains(ops, o => o.Kind == "write" && o.Outcome == OpOutcome.Ok);
                LinearizabilityChecker.Check(ops, new RegisterModel(initial[key])).AssertValid();
            }

            var keyVersions = new SortedSet<uint>();
            foreach (TestNode node in cluster.Nodes)
            {
                foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
                    keyVersions.Add(record.KeyVersion);
            }

            Assert.True(keyVersions.Count >= 2, $"objects of several key versions must coexist after rotation; found [{string.Join(", ", keyVersions)}]");

            await using (DfsSnapshotView pre = await adminVol.Snapshots.OpenAsync("pre-rotation", Ct))
            {
                foreach ((string key, byte[] bytes) in initial)
                    await RecoveryScenario.AssertFileBytesAsync(pre.Files, key, bytes, "the pre-rotation snapshot stays decodable with its original key", Ct);
            }

            await admin.Admin.StartScrubAsync(null, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            NodeCounters counters = await cluster.Inspect.GetTotalCountersAsync();
            Assert.Equal(0L, counters.ChecksumFailures);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-017
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>The wall-clock change in SEC-017.</summary>
    public enum ClockChange
    {
        /// <summary>The writer client's clock stops (no renewals) and then jumps one hour back.</summary>
        ClientWallClockBackward,

        /// <summary>The writer client's clock is frozen beyond the session timeout.</summary>
        ClientClockFrozenPastSessionTimeout,

        /// <summary>The volume leader's wall clock jumps one hour back while the writer is silent.</summary>
        LeaderWallClockBackward,

        /// <summary>The volume leader's wall clock jumps one day forward while the writer keeps renewing.</summary>
        LeaderWallClockForward,

        /// <summary>A storage node's wall clock jumps back after a staging capability expired; the upload is replayed.</summary>
        StorageNodeWallClockBackwardReplay,
    }

    /// <summary>
    /// Moves client and server wall clocks across lease, session and capability validity boundaries: expired authority is
    /// never revived — not by the clock change and not by re-authenticating the old client's connections afterwards (the old
    /// writer's publication fails with LeaseLost and a replacement writer wins), valid authority is not falsely expired by
    /// forward jumps (the renewed writer still publishes), at most one writer is valid at every observation, and replayed
    /// uploads never become published content.
    /// </summary>
    [Theory]
    [InlineData(ClockChange.ClientWallClockBackward)]
    [InlineData(ClockChange.ClientClockFrozenPastSessionTimeout)]
    [InlineData(ClockChange.LeaderWallClockBackward)]
    [InlineData(ClockChange.LeaderWallClockForward)]
    [InlineData(ClockChange.StorageNodeWallClockBackwardReplay)]
    [Catalog("SEC-017", "S", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_017_ClockChangeCertificateBehavior(ClockChange change) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            const string path = "/sec17/f.bin";
            NodeTimings timings = cluster.Options.Timings;
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            byte[] original = TestData.Bytes(1701, 2 * chunk + 1);
            await adminVol.Directories.CreateDirectoryAsync("/sec17", Ct);
            await adminVol.Files.WriteAllBytesAsync(path, original, Ct);

            var clockA = new TestClock(cluster.Time);
            await using DfsClient a = await cluster.ConnectAsync(new TestClientOptions { Name = "a", Clock = clockA }, Ct);
            await using DfsClient b = await cluster.ConnectAsync(new TestClientOptions { Name = "b" }, Ct);
            DfsVolume av = a.GetVolume(Vol);
            DfsVolume bv = b.GetVolume(Vol);
            byte[] aBytes = TestData.Bytes(1702, 5000);
            byte[] bBytes = TestData.Bytes(1703, 7000);

            async Task<CommitToken> ReplaceAsWriterBAsync()
            {
                await using DfsStream s = await bv.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                await s.SetLengthAsync(0, Ct);
                await s.WriteAsync(bBytes, Ct);
                return await s.CommitAsync(Ct);
            }

            if (change == ClockChange.StorageNodeWallClockBackwardReplay)
            {
                CapturedMessage captured;
                await using (DfsStream staging = await av.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct))
                {
                    using MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:a", Type: MessageType.PutObject, Direction: LinkDirection.Request));
                    Task write = staging.WriteAsync(TestData.Bytes(1704, 3 * chunk), Ct).AsTask();
                    await hold.WaitForAsync(1, TimeSpan.FromSeconds(30));
                    captured = hold.Captured[0];
                    hold.ReleaseAll();
                    await write;
                    await staging.AbortAsync(Ct);
                }

                await cluster.DelayAsync(timings.PinDuration * 3 + timings.LeaseDuration * 3, Ct);
                cluster.Node(captured.To).Clock.JumpWall(TimeSpan.FromHours(-1));
                await using RawProtocolClient raw = await cluster.ConnectRawAsync(captured.To, Ct);
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                DfsErrorCode code = await raw.SendFrameAsync(captured.Frame);
                Assert.True(code is DfsErrorCode.None or DfsErrorCode.AccessDenied or DfsErrorCode.PinExpired or DfsErrorCode.InvalidHandle or DfsErrorCode.LeaseLost,
                    $"replayed expired upload: unexpected {code}");
                await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, path, original, "a replayed upload never becomes published content", Ct);
                CommitToken bCommit = await ReplaceAsWriterBAsync();
                Assert.True(bCommit.IsComparableWith((await adminVol.GetInfoAsync(Ct)).LastCommit));
                await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, path, bBytes, "the replacement writer publishes normally", Ct);
                return;
            }

            DfsStream writerA = await av.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            try
            {
                await writerA.WriteAsync(aBytes, Ct);
                await SecHelpers.AssertAtMostOneWriterAsync(cluster, Vol, path, "after A opened");
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                switch (change)
                {
                    case ClockChange.ClientWallClockBackward:
                        clockA.Freeze();
                        await cluster.DelayAsync(timings.LeaseDuration * 3, Ct);
                        clockA.JumpWall(TimeSpan.FromHours(-1));
                        clockA.Unfreeze();
                        break;
                    case ClockChange.ClientClockFrozenPastSessionTimeout:
                        clockA.Freeze();
                        await cluster.DelayAsync(timings.SessionTimeout * 2, Ct);
                        clockA.Unfreeze();
                        break;
                    case ClockChange.LeaderWallClockBackward:
                        clockA.Freeze();
                        leader.Clock.JumpWall(TimeSpan.FromHours(-1));
                        await cluster.DelayAsync(timings.LeaseDuration * 3, Ct);
                        clockA.Unfreeze();
                        break;
                    default:
                        leader.Clock.JumpWall(TimeSpan.FromDays(1));
                        await cluster.DelayAsync(timings.LeaseDuration * 3, Ct);
                        break;
                }

                await SecHelpers.AssertAtMostOneWriterAsync(cluster, Vol, path, $"after {change}");
                if (change == ClockChange.LeaderWallClockForward)
                {
                    Exception? second = await RecoveryScenario.CaptureAsync(() => ReplaceAsWriterBAsync());
                    Assert.True(second is DfsSharingViolationException, $"A still renews its lease; a forward wall-clock jump must not hand writer authority to B (got {second?.GetType().Name ?? "success"})");

                    // Leases are tracked on the leader's monotonic clock (ARCHITECTURE §5.3): a wall-clock jump neither revives
                    // nor falsely expires authority, so the still-valid writer A publishes normally.
                    CommitToken aCommit = await writerA.CommitAsync(Ct);
                    Assert.False(writerA.IsFaulted, "a forward wall-clock jump must not fault a writer whose lease is still renewed");
                    Assert.True(aCommit.IsComparableWith((await adminVol.GetInfoAsync(Ct)).LastCommit));
                    await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, path, [.. aBytes, .. original.AsSpan(aBytes.Length)], "forward wall-clock jump", Ct);
                }
                else
                {
                    // A's connections re-authenticate under the changed clock; authentication must not revive the expired
                    // writer authority (or hand the old lease to the re-authenticated session).
                    cluster.Network.ResetConnections("client:a");
                    await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                    await SecHelpers.AssertAtMostOneWriterAsync(cluster, Vol, path, $"after A re-authenticated following {change}");

                    CommitToken bCommit = await ReplaceAsWriterBAsync();
                    await SecHelpers.AssertAtMostOneWriterAsync(cluster, Vol, path, "after B replaced A");
                    Exception? stale = await RecoveryScenario.CaptureAsync(() => writerA.CommitAsync(Ct));
                    Assert.True(stale is DfsLeaseLostException or DfsOperationExpiredException or DfsPinExpiredException,
                        $"the expired writer must not regain authority through a clock change, got {stale?.GetType().Name ?? "success"}");
                    if (stale is DfsLeaseLostException)
                        Assert.True(writerA.IsFaulted, "lease loss must fault the expired writer's stream (FSD §6.4)");

                    // The SDK must not silently reacquire authority and publish A's stale buffer on a later attempt either.
                    Exception? staleAgain = await RecoveryScenario.CaptureAsync(async () =>
                    {
                        await writerA.WriteAsync(aBytes.AsMemory(0, 10), Ct);
                        await writerA.CommitAsync(Ct);
                    });
                    Assert.True(staleAgain is not null, "a second publication attempt by the expired writer succeeded");
                    await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, path, bBytes, "the replacement writer's content stays authoritative", Ct);
                    Assert.True(bCommit.IsComparableWith((await adminVol.GetInfoAsync(Ct)).LastCommit));

                    if (change == ClockChange.ClientClockFrozenPastSessionTimeout)
                    {
                        byte[] fresh = TestData.Bytes(1705, 300);
                        Exception? newWork = await RecoveryScenario.CaptureAsync(() => av.Files.WriteAllBytesAsync("/sec17/after-session.bin", fresh, Ct));
                        if (newWork is null)
                            await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, "/sec17/after-session.bin", fresh, "new work runs in a new session", Ct);
                        else
                            Assert.True(newWork is DfsOperationExpiredException or DfsLeaseLostException, $"new work after session expiry: {newWork}");
                    }
                }
            }
            finally
            {
                await RecoveryScenario.CaptureAsync(async () => await writerA.DisposeAsync());
            }

            await SecHelpers.AssertAtMostOneWriterAsync(cluster, Vol, path, "at the end");
        }, Ct);
}
