using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.SnpApp;

namespace Tedd.Tdfs.Tests.Unit.Snp;

/// <summary>
/// SNP unit-level contract tests (test plan §7.19, FSD §11): snapshot views are immutable, and minimum-boundary tokens
/// from other volumes, clusters or authority epochs are rejected rather than ordered.
/// </summary>
public sealed class SnpUnitTests
{
    private const string Volume = "vol";
    private const string SnapshotName = "ro";
    private const string StrongPolicy = "strong";
    private static readonly int Chunk = Tedd.Tdfs.Server.NodeTimings.Fast.ChunkSize;
    private static readonly byte[] BigBytes = TestData.Bytes(0x5_004_001, (3 * Chunk) + 17);
    private static readonly byte[] SmallBytes = TestData.Bytes(0x5_004_002, 100);
    private static readonly byte[] SmallBytesV2 = TestData.Bytes(0x5_004_003, 120);
    private static readonly byte[] Payload = TestData.Bytes(0x5_004_004, 333);

    // ------------------------------------------------------------------------------------------------------------
    // SNP-004 SnapshotIsImmutable
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Every mutating facade API reachable from a snapshot view (async and synchronous counterparts).</summary>
    private static readonly IReadOnlyDictionary<string, Func<DfsSnapshotView, CancellationToken, Task>> FacadeMutations =
        new Dictionary<string, Func<DfsSnapshotView, CancellationToken, Task>>(StringComparer.Ordinal)
        {
            ["Files.WriteAllBytesAsync(existing)"] = (v, ct) => v.Files.WriteAllBytesAsync("/ro/big", Payload, ct),
            ["Files.WriteAllBytesAsync(new)"] = (v, ct) => v.Files.WriteAllBytesAsync("/ro/new", Payload, ct),
            ["Files.WriteAllBytesAsync(options+commit-snapshot)"] = (v, ct) =>
                v.Files.WriteAllBytesAsync("/ro/big", Payload, new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Files.WriteAllBytes(sync)"] = Sync(v => v.Files.WriteAllBytes("/ro/big", Payload)),
            ["Files.WriteAllTextAsync"] = (v, ct) => v.Files.WriteAllTextAsync("/ro/small", "text", null, ct),
            ["Files.WriteAllText(sync)"] = Sync(v => v.Files.WriteAllText("/ro/small", "text")),
            ["Files.WriteAllLinesAsync"] = (v, ct) => v.Files.WriteAllLinesAsync("/ro/small", ["a", "b"], null, ct),
            ["Files.WriteAllLines(sync)"] = Sync(v => v.Files.WriteAllLines("/ro/small", ["a", "b"])),
            ["Files.AppendAllBytesAsync"] = (v, ct) => v.Files.AppendAllBytesAsync("/ro/small", Payload, ct),
            ["Files.AppendAllBytesAsync(new)"] = (v, ct) => v.Files.AppendAllBytesAsync("/ro/new", Payload, ct),
            ["Files.AppendAllBytesAsync(options)"] = (v, ct) =>
                v.Files.AppendAllBytesAsync("/ro/small", Payload, new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Files.AppendAllBytes(sync)"] = Sync(v => v.Files.AppendAllBytes("/ro/small", Payload)),
            ["Files.AppendAllTextAsync"] = (v, ct) => v.Files.AppendAllTextAsync("/ro/small", "tail", null, ct),
            ["Files.AppendAllText(sync)"] = Sync(v => v.Files.AppendAllText("/ro/small", "tail")),
            ["Files.AppendAllLinesAsync"] = (v, ct) => v.Files.AppendAllLinesAsync("/ro/small", ["x"], null, ct),
            ["Files.AppendAllLines(sync)"] = Sync(v => v.Files.AppendAllLines("/ro/small", ["x"])),
            ["Files.CopyAsync(new destination)"] = (v, ct) => v.Files.CopyAsync("/ro/big", "/ro/copy", false, ct),
            ["Files.CopyAsync(overwrite)"] = (v, ct) => v.Files.CopyAsync("/ro/big", "/ro/small", true, ct),
            ["Files.CopyAsync(options)"] = (v, ct) =>
                v.Files.CopyAsync("/ro/big", "/ro/copy", false, new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Files.Copy(sync)"] = Sync(v => v.Files.Copy("/ro/big", "/ro/copy")),
            ["Files.MoveAsync(rename)"] = (v, ct) => v.Files.MoveAsync("/ro/big", "/ro/moved", false, ct),
            ["Files.MoveAsync(overwrite)"] = (v, ct) => v.Files.MoveAsync("/ro/big", "/ro/small", true, ct),
            ["Files.MoveAsync(options)"] = (v, ct) =>
                v.Files.MoveAsync("/ro/big", "/ro/moved", false, new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Files.Move(sync)"] = Sync(v => v.Files.Move("/ro/big", "/ro/moved")),
            ["Files.DeleteAsync(existing)"] = (v, ct) => v.Files.DeleteAsync("/ro/big", ct),
            ["Files.DeleteAsync(absent)"] = (v, ct) => v.Files.DeleteAsync("/ro/absent", ct),
            ["Files.DeleteAsync(options)"] = (v, ct) =>
                v.Files.DeleteAsync("/ro/big", new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Files.Delete(sync)"] = Sync(v => v.Files.Delete("/ro/big")),
            ["Files.SetStoragePolicyAsync"] = (v, ct) => v.Files.SetStoragePolicyAsync("/ro/big", StrongPolicy, ct),
            ["Files.SetStoragePolicy(sync)"] = Sync(v => v.Files.SetStoragePolicy("/ro/big", StrongPolicy)),
            ["Files.SetAccessAsync"] = (v, ct) => v.Files.SetAccessAsync("/ro/big", new DfsInodeAccess(OwnerOnly: true), ct),
            ["Files.SetAccess(sync)"] = Sync(v => v.Files.SetAccess("/ro/big", new DfsInodeAccess(ReadOnlyForOthers: true))),
            ["Files.CreateAsync(existing)"] = (v, ct) => v.Files.CreateAsync("/ro/big", ct),
            ["Files.CreateAsync(new)"] = (v, ct) => v.Files.CreateAsync("/ro/new", ct),
            ["Files.Create(sync)"] = Sync(v => v.Files.Create("/ro/big").Dispose()),
            ["Files.OpenWriteAsync(existing)"] = (v, ct) => v.Files.OpenWriteAsync("/ro/big", ct),
            ["Files.OpenWriteAsync(new)"] = (v, ct) => v.Files.OpenWriteAsync("/ro/new", ct),
            ["Files.OpenWrite(sync)"] = Sync(v => v.Files.OpenWrite("/ro/big").Dispose()),
            ["Files.OpenAsync(CreateNew,Write)"] = (v, ct) => v.Files.OpenAsync("/ro/new", FileMode.CreateNew, FileAccess.Write, FileShare.None, ct),
            ["Files.OpenAsync(Create,ReadWrite)"] = (v, ct) => v.Files.OpenAsync("/ro/big", FileMode.Create, FileAccess.ReadWrite, FileShare.Read, ct),
            ["Files.OpenAsync(OpenOrCreate,Write,existing)"] = (v, ct) => v.Files.OpenAsync("/ro/big", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, ct),
            ["Files.OpenAsync(OpenOrCreate,Read,absent)"] = (v, ct) => v.Files.OpenAsync("/ro/absent", FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read, ct),
            ["Files.OpenAsync(Open,ReadWrite)"] = (v, ct) => v.Files.OpenAsync("/ro/big", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct),
            ["Files.OpenAsync(Open,Write)"] = (v, ct) => v.Files.OpenAsync("/ro/big", FileMode.Open, FileAccess.Write, FileShare.Read, ct),
            ["Files.OpenAsync(Truncate,Write)"] = (v, ct) => v.Files.OpenAsync("/ro/big", FileMode.Truncate, FileAccess.Write, FileShare.Read, ct),
            ["Files.OpenAsync(Append,Write)"] = (v, ct) => v.Files.OpenAsync("/ro/small", FileMode.Append, FileAccess.Write, FileShare.Read, ct),
            ["Files.OpenAsync(options,WriteThrough,ReadWrite)"] = (v, ct) => v.Files.OpenAsync("/ro/big", new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.ReadWrite,
                Share = FileShare.Read,
                WriteMode = DfsWriteMode.WriteThrough,
            }, ct),
            ["Files.Open(sync,Open,ReadWrite)"] = Sync(v => v.Files.Open("/ro/big", FileMode.Open, FileAccess.ReadWrite, FileShare.Read).Dispose()),
            ["Files.Open(sync options,Create)"] = Sync(v => v.Files.Open("/ro/new", new DfsOpenOptions { Mode = FileMode.Create, Access = FileAccess.Write }).Dispose()),
            ["Directories.CreateDirectoryAsync(new)"] = (v, ct) => v.Directories.CreateDirectoryAsync("/ro/newdir/sub", ct),
            ["Directories.CreateDirectoryAsync(existing)"] = (v, ct) => v.Directories.CreateDirectoryAsync("/ro/dir", ct),
            ["Directories.CreateDirectoryAsync(options)"] = (v, ct) =>
                v.Directories.CreateDirectoryAsync("/ro/newdir", new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Directories.CreateDirectory(sync)"] = Sync(v => v.Directories.CreateDirectory("/ro/newdir")),
            ["Directories.MoveAsync"] = (v, ct) => v.Directories.MoveAsync("/ro/dir", "/ro/dir2", ct),
            ["Directories.MoveAsync(options)"] = (v, ct) =>
                v.Directories.MoveAsync("/ro/dir", "/ro/dir2", new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Directories.Move(sync)"] = Sync(v => v.Directories.Move("/ro/dir", "/ro/dir2")),
            ["Directories.DeleteAsync(empty)"] = (v, ct) => v.Directories.DeleteAsync("/ro/emptydir", false, ct),
            ["Directories.DeleteAsync(recursive)"] = (v, ct) => v.Directories.DeleteAsync("/ro/dir", true, ct),
            ["Directories.DeleteAsync(options)"] = (v, ct) =>
                v.Directories.DeleteAsync("/ro/dir", true, new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Directories.Delete(sync)"] = Sync(v => v.Directories.Delete("/ro/emptydir")),
        };

    /// <summary>Every mutating stream member, invoked on a read stream opened through a snapshot view.</summary>
    private static readonly IReadOnlyDictionary<string, Func<DfsStream, CancellationToken, Task>> StreamMutations =
        new Dictionary<string, Func<DfsStream, CancellationToken, Task>>(StringComparer.Ordinal)
        {
            ["Stream.Write(byte[],int,int)"] = SyncStream(s => s.Write(Payload, 0, Payload.Length)),
            ["Stream.Write(ReadOnlySpan)"] = SyncStream(s => s.Write(Payload.AsSpan())),
            ["Stream.WriteAsync(byte[],int,int)"] = (s, ct) => s.WriteAsync(Payload, 0, Payload.Length, ct),
            ["Stream.WriteAsync(ReadOnlyMemory)"] = (s, ct) => s.WriteAsync(Payload.AsMemory(), ct).AsTask(),
            ["Stream.WriteByte"] = SyncStream(s => s.WriteByte(0x42)),
            ["Stream.SetLength(shrink)"] = SyncStream(s => s.SetLength(10)),
            ["Stream.SetLength(extend)"] = SyncStream(s => s.SetLength(BigBytes.Length + 1000L)),
            ["Stream.SetLengthAsync"] = (s, ct) => s.SetLengthAsync(10, ct),
            ["Stream.CommitAsync"] = (s, ct) => s.CommitAsync(ct),
            ["Stream.CommitAsync(options+commit-snapshot)"] = (s, ct) => s.CommitAsync(new DfsMutationOptions { CommitSnapshotName = "ro-extra" }, ct),
            ["Stream.Commit(sync)"] = SyncStream(s => s.Commit()),
            ["Stream as CopyToAsync destination"] = async (s, ct) =>
            {
                using var source = new MemoryStream(Payload);
                await source.CopyToAsync(s, ct);
            },
        };

    public static TheoryData<string> FacadeMutationNames()
    {
        var data = new TheoryData<string>();
        foreach (string name in FacadeMutations.Keys)
            data.Add(name);
        return data;
    }

    public static TheoryData<string> StreamMutationNames()
    {
        var data = new TheoryData<string>();
        foreach (string name in StreamMutations.Keys)
            data.Add(name);
        return data;
    }

    /// <summary>
    /// SNP-004: every mutating file/directory facade member of a snapshot view is rejected with
    /// <see cref="DfsReadOnlyException"/> (ErrorCode ReadOnly) before any visible mutation of the snapshot, the live
    /// volume, the snapshot list or the writer reservations.
    /// </summary>
    [Theory]
    [MemberData(nameof(FacadeMutationNames))]
    [Catalog("SNP-004", "U", "INV-02", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_004_SnapshotIsImmutable(string mutation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Func<DfsSnapshotView, CancellationToken, Task> act = FacadeMutations[mutation];
        await ClusterTest.RunAsync(ImmutableOptions(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            await SeedImmutableFixtureAsync(vol, ct);

            await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(SnapshotName, ct);
            Assert.True(view.Files.IsReadOnly, "snapshot view file facade must report IsReadOnly");
            Assert.True(view.Directories.IsReadOnly, "snapshot view directory facade must report IsReadOnly");
            Assert.False(view.Files.IsTransactionBound);

            Baseline before = await CaptureBaselineAsync(cluster, vol, view, ct);

            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(() => act(view, ct), DfsErrorCode.ReadOnly);

            await AssertUnchangedAsync(cluster, vol, view, before, mutation, ct);
        }, ct);
    }

    /// <summary>
    /// SNP-004 (stream members): a stream opened through a snapshot view is read-only; every mutating stream member is
    /// rejected (NotSupportedException per the Stream contract, or DfsReadOnlyException) and nothing becomes visible; the
    /// stream still reads the exact snapshot bytes afterwards.
    /// </summary>
    [Theory]
    [MemberData(nameof(StreamMutationNames))]
    [Catalog("SNP-004", "U", "INV-02", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_004_SnapshotIsImmutable_StreamMembers(string mutation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Func<DfsStream, CancellationToken, Task> act = StreamMutations[mutation];
        await ClusterTest.RunAsync(ImmutableOptions(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            await SeedImmutableFixtureAsync(vol, ct);

            await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(SnapshotName, ct);
            Baseline before = await CaptureBaselineAsync(cluster, vol, view, ct);

            await using (DfsStream stream = await view.Files.OpenReadAsync("/ro/big", ct))
            {
                Assert.True(stream.CanRead);
                Assert.False(stream.CanWrite, "a snapshot stream must not be writable");
                Assert.Equal(0UL, stream.WriterGeneration);

                Exception? error = await CaptureAsync(() => act(stream, ct));
                Assert.NotNull(error);
                Assert.True(error is NotSupportedException or DfsReadOnlyException or InvalidOperationException,
                    $"'{mutation}' on a snapshot stream must be rejected as unsupported/read-only, got {error}");
                if (error is DfsException dfs)
                    Assert.Equal(DfsErrorCode.ReadOnly, dfs.ErrorCode);

                // The stream still serves the exact snapshot revision from offset zero (no partial staged state).
                stream.Position = 0;
                byte[] reread = new byte[BigBytes.Length + 1];
                int total = 0;
                int n;
                while ((n = await stream.ReadAsync(reread.AsMemory(total), ct)) > 0)
                    total += n;
                AssertEx.BytesEqual(BigBytes, reread.AsSpan(0, total), "snapshot stream after a rejected mutation");

                // Read-only flush is a no-op (FSD §6.4) and must not publish anything.
                await stream.FlushAsync(ct);
            }

            await AssertUnchangedAsync(cluster, vol, view, before, mutation, ct);
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // SNP-010 ForeignEpochTokenRejected
    // ------------------------------------------------------------------------------------------------------------

    public enum ForeignToken
    {
        OtherVolume,
        RestoredVolume,
        OtherCluster,
        NextEpoch,
        FarFutureEpoch,
    }

    /// <summary>
    /// SNP-010: a minimum-boundary token from another volume, a restored volume, another cluster or another authority
    /// epoch is rejected with the typed cross-volume / epoch / cluster error; no snapshot is created, nothing is ordered
    /// against the foreign token, and <see cref="CommitToken.CompareTo"/> refuses to order it against real boundaries.
    /// </summary>
    [Theory]
    [InlineData(ForeignToken.OtherVolume, false)]
    [InlineData(ForeignToken.RestoredVolume, false)]
    [InlineData(ForeignToken.OtherCluster, false)]
    [InlineData(ForeignToken.NextEpoch, false)]
    [InlineData(ForeignToken.FarFutureEpoch, false)]
    [InlineData(ForeignToken.OtherVolume, true)]
    [InlineData(ForeignToken.NextEpoch, true)]
    [Catalog("SNP-010", "U", "INV-02", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_010_ForeignEpochTokenRejected(ForeignToken kind, bool syncApi)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(TwoVolumeOptions(backups: 0), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(5) }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            (CommitToken own, CommitToken foreign, DfsSnapshotInfo baseline) = await PrepareForeignTokenAsync(vol, client, kind, ct);

            IReadOnlyList<DfsSnapshotInfo> listBefore = await vol.Snapshots.ListAsync(ct);
            var options = new DfsSnapshotOptions { MinimumCommit = foreign };
            Exception? error = await CaptureAsync(() => syncApi
                ? Task.Run(() => vol.Snapshots.Create("foreign", options), ct)
                : vol.Snapshots.CreateAsync("foreign", options, ct));

            AssertForeignRejection(kind, error);

            // Nothing was created or ordered against the foreign token.
            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("foreign", ct), DfsErrorCode.SnapshotNotFound);
            IReadOnlyList<DfsSnapshotInfo> listAfter = await vol.Snapshots.ListAsync(ct);
            Assert.Equal(listBefore.Select(s => s.Id).OrderBy(i => i.Value), listAfter.Select(s => s.Id).OrderBy(i => i.Value));
            Assert.False(foreign.IsComparableWith(baseline.Boundary), "the foreign token must not be comparable with this volume's boundaries");
            Assert.Throws<ArgumentException>(() => foreign.CompareTo(baseline.Boundary));
            Assert.Throws<ArgumentException>(() => baseline.Boundary.IsAtOrAfter(foreign));

            // Positive control: the volume's own token is honoured (same epoch, at or after it).
            DfsSnapshotInfo ok = await vol.Snapshots.CreateAsync("own", new DfsSnapshotOptions { MinimumCommit = own }, ct);
            Assert.True(ok.Boundary.IsComparableWith(own));
            Assert.True(ok.Boundary.IsAtOrAfter(own), $"own-token snapshot boundary {ok.Boundary} must be at or after {own}");
        }, ct);
    }

    /// <summary>
    /// SNP-010 (retention claims): a backup barrier for a foreign token is rejected with the typed error instead of
    /// returning a checkpoint or timing out as if the token were merely not yet reached; the volume's own token still
    /// yields a checkpoint at or after it.
    /// </summary>
    [Theory]
    [InlineData(ForeignToken.OtherVolume)]
    [InlineData(ForeignToken.OtherCluster)]
    [InlineData(ForeignToken.NextEpoch)]
    [InlineData(ForeignToken.FarFutureEpoch)]
    [Catalog("SNP-010", "U", "INV-02", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_010_ForeignEpochTokenRejected_BackupBarrier(ForeignToken kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(TwoVolumeOptions(backups: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: false));
            (CommitToken own, CommitToken foreign, DfsSnapshotInfo baseline) = await PrepareForeignTokenAsync(vol, client, kind, ct);

            await vol.Backups.RequestSyncAsync(backup.TargetName, baseline.Name, ct);
            DfsBackupCheckpoint published = await vol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, baseline.Name, TimeSpan.FromSeconds(60), ct);
            Assert.Equal(baseline.Id, published.SnapshotId);

            Exception? error = await CaptureAsync(() => vol.Backups.AwaitBackupAsync(backup.TargetName, foreign, TimeSpan.FromSeconds(3), ct));
            Assert.False(error is DfsBackupNotReachedException, $"a foreign token must be rejected, not reported as 'not reached yet': {error}");
            AssertForeignRejection(kind, error);

            // Positive control: the volume's own token (covered by the published baseline) is acknowledged.
            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupAsync(backup.TargetName, own, TimeSpan.FromSeconds(60), ct);
            Assert.True(checkpoint.CoveredCommit.IsComparableWith(own));
            Assert.True(checkpoint.CoveredCommit.IsAtOrAfter(own));
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------------------

    private sealed record Baseline(
        SortedDictionary<string, byte[]?> SnapshotTree,
        SortedDictionary<string, byte[]?> LiveTree,
        DfsFileInfo LiveBig,
        DfsFileInfo SnapshotBig,
        IReadOnlyList<string> SnapshotNames);

    private static TestClusterOptions ImmutableOptions() => TestClusterOptions.ThreeZones with
    {
        Volumes =
        [
            new TestVolumeSpec(Volume)
            {
                NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal)
                {
                    [StrongPolicy] = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 },
                },
            },
        ],
    };

    private static TestClusterOptions TwoVolumeOptions(int backups) => TestClusterOptions.ThreeZones with
    {
        Volumes = [new TestVolumeSpec(Volume), new TestVolumeSpec("vol2")],
        BackupNodeCount = backups,
    };

    /// <summary>Live: /ro/big (multi-extent), /ro/small, /ro/dir/x, /ro/emptydir; snapshot "ro"; then live /ro/small changes.</summary>
    private static async Task SeedImmutableFixtureAsync(DfsVolume vol, CancellationToken ct)
    {
        await vol.Directories.CreateDirectoryAsync("/ro/dir", ct);
        await vol.Directories.CreateDirectoryAsync("/ro/emptydir", ct);
        await vol.Files.WriteAllBytesAsync("/ro/big", BigBytes, ct);
        await vol.Files.WriteAllBytesAsync("/ro/small", SmallBytes, ct);
        await vol.Files.WriteAllBytesAsync("/ro/dir/x", SmallBytes, ct);
        DfsSnapshotInfo info = await vol.Snapshots.CreateAsync(SnapshotName, null, ct);
        Assert.Equal(SnapshotName, info.Name);
        await vol.Files.WriteAllBytesAsync("/ro/small", SmallBytesV2, ct);
    }

    private static async Task<Baseline> CaptureBaselineAsync(TestCluster cluster, DfsVolume vol, DfsSnapshotView view, CancellationToken ct)
    {
        SortedDictionary<string, byte[]?> snapshotTree = await SnapshotTree.CaptureAsync(view, "/", ct);
        SortedDictionary<string, byte[]?> liveTree = await SnapshotTree.CaptureAsync(vol, "/", ct);
        Assert.True(snapshotTree.ContainsKey("/ro/emptydir"));
        AssertEx.BytesEqual(BigBytes, snapshotTree["/ro/big"], "fixture");
        AssertEx.BytesEqual(SmallBytes, snapshotTree["/ro/small"], "snapshot keeps the pre-snapshot version");
        AssertEx.BytesEqual(SmallBytesV2, liveTree["/ro/small"], "live has the post-snapshot version");
        DfsFileInfo liveBig = await vol.Files.GetInfoAsync("/ro/big", ct);
        DfsFileInfo snapshotBig = await view.Files.GetInfoAsync("/ro/big", ct);
        IReadOnlyList<string> names = (await vol.Snapshots.ListAsync(ct)).Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
        Assert.DoesNotContain(state.Handles, h => h.IsWriter && !h.Expired);
        return new Baseline(snapshotTree, liveTree, liveBig, snapshotBig, names);
    }

    private static async Task AssertUnchangedAsync(TestCluster cluster, DfsVolume vol, DfsSnapshotView view, Baseline before, string mutation,
        CancellationToken ct)
    {
        SnapshotTree.AssertEqual(before.SnapshotTree, await SnapshotTree.CaptureAsync(view, "/", ct), $"snapshot after rejected '{mutation}'");
        SnapshotTree.AssertEqual(before.LiveTree, await SnapshotTree.CaptureAsync(vol, "/", ct), $"live volume after rejected '{mutation}'");

        DfsFileInfo liveBig = await vol.Files.GetInfoAsync("/ro/big", ct);
        Assert.Equal(before.LiveBig.InodeId, liveBig.InodeId);
        Assert.Equal(before.LiveBig.Revision, liveBig.Revision);
        Assert.Equal(before.LiveBig.Access, liveBig.Access);
        Assert.Equal(before.LiveBig.StoragePolicy, liveBig.StoragePolicy);
        DfsFileInfo snapshotBig = await view.Files.GetInfoAsync("/ro/big", ct);
        Assert.Equal(before.SnapshotBig.InodeId, snapshotBig.InodeId);
        Assert.Equal(before.SnapshotBig.Revision, snapshotBig.Revision);
        Assert.Equal(before.SnapshotBig.Access, snapshotBig.Access);
        Assert.Equal(before.SnapshotBig.StoragePolicy, snapshotBig.StoragePolicy);

        IReadOnlyList<string> names = (await vol.Snapshots.ListAsync(ct)).Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(before.SnapshotNames, names);

        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
        Assert.DoesNotContain(state.Handles, h => h.IsWriter && !h.Expired);
        Assert.Equal(0, state.ActiveTransactions);
    }

    private static async Task<(CommitToken Own, CommitToken Foreign, DfsSnapshotInfo Baseline)> PrepareForeignTokenAsync(
        DfsVolume vol, DfsClient client, ForeignToken kind, CancellationToken ct)
    {
        CommitToken own = await vol.Files.WriteAllBytesAsync("/f/own", TestData.Bytes(0x5_010_001, 2000), ct);
        Assert.False(own.IsNone);
        DfsSnapshotInfo baseline = await vol.Snapshots.CreateAsync("baseline", null, ct);
        Assert.True(baseline.Boundary.IsAtOrAfter(own));

        CommitToken foreign;
        switch (kind)
        {
            case ForeignToken.OtherVolume:
                foreign = await client.GetVolume("vol2").Files.WriteAllBytesAsync("/f/other", TestData.Bytes(0x5_010_002, 2000), ct);
                Assert.NotEqual(own.VolumeId, foreign.VolumeId);
                Assert.Equal(own.ClusterId, foreign.ClusterId);
                break;
            case ForeignToken.RestoredVolume:
                {
                    DfsVolumeInfo restored = await vol.Snapshots.RestoreAsync(baseline.Name, "vol-restored", ct);
                    Assert.NotEqual(own.VolumeId, restored.Id);
                    foreign = await client.GetVolume("vol-restored").Files.WriteAllBytesAsync("/f/restored", TestData.Bytes(0x5_010_003, 2000), ct);
                    Assert.Equal(restored.Id, foreign.VolumeId);
                    break;
                }

            case ForeignToken.OtherCluster:
                foreign = own with { ClusterId = new Guid("5e1ec7ed-0000-4000-8000-00000000f0f0") };
                break;
            case ForeignToken.NextEpoch:
                foreign = own with { VolumeEpoch = own.VolumeEpoch + 1 };
                break;
            case ForeignToken.FarFutureEpoch:
                foreign = own with { VolumeEpoch = own.VolumeEpoch + 1000, Sequence = 1 };
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }

        Assert.False(foreign.IsNone);
        Assert.False(foreign.IsComparableWith(own), $"{kind}: the test token must belong to another history");
        return (own, foreign, baseline);
    }

    private static void AssertForeignRejection(ForeignToken kind, Exception? error)
    {
        Assert.NotNull(error);
        DfsErrorCode[] allowed = kind switch
        {
            ForeignToken.OtherVolume or ForeignToken.RestoredVolume => [DfsErrorCode.CrossVolume],
            ForeignToken.NextEpoch or ForeignToken.FarFutureEpoch => [DfsErrorCode.EpochMismatch],
            ForeignToken.OtherCluster => [DfsErrorCode.ClusterMismatch, DfsErrorCode.CrossVolume],
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };
        DfsException dfs = Assert.IsAssignableFrom<DfsException>(error);
        Assert.True(allowed.Contains(dfs.ErrorCode), $"{kind}: expected one of [{string.Join(", ", allowed)}] but got {dfs.ErrorCode}: {error}");
        Type expectedType = dfs.ErrorCode switch
        {
            DfsErrorCode.CrossVolume => typeof(DfsCrossVolumeException),
            DfsErrorCode.EpochMismatch => typeof(DfsEpochMismatchException),
            DfsErrorCode.ClusterMismatch => typeof(DfsClusterIdentityException),
            _ => typeof(DfsException),
        };
        Assert.IsAssignableFrom(expectedType, error);
        Assert.False(dfs.IsRetryable, $"{kind}: a foreign token can never become valid by retrying");
    }

    private static Func<DfsSnapshotView, CancellationToken, Task> Sync(Action<DfsSnapshotView> action) =>
        (v, ct) => Task.Run(() => action(v), ct);

    private static Func<DfsStream, CancellationToken, Task> SyncStream(Action<DfsStream> action) =>
        (s, ct) => Task.Run(() => action(s), ct);

    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
