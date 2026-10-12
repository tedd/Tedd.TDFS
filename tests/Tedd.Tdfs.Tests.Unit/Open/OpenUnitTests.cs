using System.Reflection;
using System.Runtime.InteropServices;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Open;

/// <summary>
/// OPEN suite, level U: open modes, access rights and handles (FSD §§5.4, 6.1, 7; test plan §7.4).
/// Mode × access × share matrices are evaluated against the independent <see cref="OpenOracle"/>; every combination is
/// labelled in the test output and every deviation is reported.
/// </summary>
public sealed class OpenUnitTests
{
    private const string Vol = OpenHelpers.Volume;

    private static CancellationToken Ct => OpenHelpers.Ct;

    private static readonly bool[] MissingThenExisting = [false, true];

    // ------------------------------------------------------------------------------------------------------------
    // Theory data
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, int> AllAccessValues => OpenHelpers.AllAccessValues;

    public static TheoryData<string, int> WritableAccessValues => new() { { "Write", (int)FileAccess.Write }, { "ReadWrite", (int)FileAccess.ReadWrite } };

    public static TheoryData<string, int> EveryDefinedAccessValue => new()
    {
        { "Read", (int)FileAccess.Read }, { "Write", (int)FileAccess.Write }, { "ReadWrite", (int)FileAccess.ReadWrite },
    };

    public static TheoryData<long> BoundaryLengths
    {
        get
        {
            var data = new TheoryData<long>();
            foreach (long length in TestData.BoundaryLengths(inlineThreshold: 4096, chunkSize: 64 * 1024, bufferSize: 64 * 1024))
                data.Add(length);
            return data;
        }
    }

    public static TheoryData<int> InvalidModeValues
    {
        get
        {
            var data = new TheoryData<int>();
            foreach (FileMode mode in OpenOracle.InvalidModes)
                data.Add((int)mode);
            return data;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-001 CreateNewAbsent
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// CreateNew × access × every share value, for a missing and an existing name. Missing + legal options: the handle
    /// and the namespace entry appear together (one committed open) with length zero; existing: AlreadyExists without
    /// change; illegal options: explicit rejection before any request.
    /// </summary>
    [Theory, Catalog("OPEN-001", "U", "INV-02", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_001_CreateNewAbsent(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"CreateNew with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open001");
            foreach (FileShare share in OpenOracle.AllShares)
            {
                foreach (bool exists in MissingThenExisting)
                    await matrix.RunAsync(FileMode.CreateNew, access, share, exists, held: []);
            }

            matrix.AssertAllPassed(OpenOracle.AllShares.Count * 2,
                OpenOracle.IsWritable(access) ? [OpenOutcome.Created, OpenOutcome.AlreadyExists, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>CreateNew against a name with active handles of every legal kind: AlreadyExists, never a sharing result, no change.</summary>
    [Theory, Catalog("OPEN-001", "U", "INV-02", "INV-07")]
    [MemberData(nameof(WritableAccessValues))]
    public async Task OPEN_001_CreateNewAbsent_ExistingNameWithActiveHandles(string accessName, int accessValue)
    {
        await RunActiveHandleMatrixAsync("/open001h", FileMode.CreateNew, (FileAccess)accessValue, accessName, OpenOutcome.AlreadyExists);
    }

    /// <summary>
    /// Every public open overload (async/sync × mode-triple/options) has the same CreateNew semantics: the entry, the
    /// handle reservation and length zero appear together; a second CreateNew fails with AlreadyExists.
    /// </summary>
    [Theory, Catalog("OPEN-001", "U", "INV-02", "INV-07")]
    [InlineData("OpenAsync(mode,access,share)")]
    [InlineData("OpenAsync(options)")]
    [InlineData("Open(mode,access,share)")]
    [InlineData("Open(options)")]
    public async Task OPEN_001_CreateNewAbsent_EveryOverload(string overload)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            await observer.GetVolume(Vol).Directories.CreateDirectoryAsync("/o1", Ct);
            const string path = "/o1/new";
            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;

            Assert.False(await obs.ExistsAsync(path, Ct));
            DfsStream stream = await OpenViaAsync(files, overload, path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
            try
            {
                // The entry is visible to another client as soon as the open returned, before any write or flush.
                Assert.True(await obs.ExistsAsync(path, Ct));
                DfsFileInfo info = await obs.GetInfoAsync(path, Ct);
                Assert.Equal(stream.InodeId, info.InodeId);
                Assert.Equal(0L, info.Length);
                Assert.Equal(0L, await stream.GetLengthAsync(Ct));
                Assert.Equal(0L, stream.Position);
                Assert.True(stream.WriterGeneration > 0);

                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                Assert.Equal(inodesBefore + 1, state.InodeCount);
                HandleInspection handle = Assert.Single(state.Handles, h => h.Inode == stream.InodeId && !h.Expired);
                Assert.True(handle.IsWriter);
                Assert.Equal(FileAccess.ReadWrite, handle.Access);
                Assert.Equal(FileShare.Read, handle.Share);
                Assert.Equal(stream.WriterGeneration, handle.Generation);

                // A second CreateNew of the same name, through the same overload, fails without touching the first.
                DfsException dup = await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(
                    () => OpenViaAsync(files, overload, path, FileMode.CreateNew, FileAccess.Write, FileShare.None), DfsErrorCode.AlreadyExists);
                Assert.Equal(DfsErrorCode.AlreadyExists, dup.ErrorCode);
                Assert.Equal(inodesBefore + 1, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);
            }
            finally
            {
                await stream.DisposeAsync();
            }

            Assert.Empty(await obs.ReadAllBytesAsync(path, Ct));
            Assert.Empty(await OpenHelpers.LiveHandlesAsync(cluster, (await obs.GetInfoAsync(path, Ct)).InodeId));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-002 is level S (OpenSimulationTests).
    // OPEN-003 CreateExistingTruncatesAtOpen
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Create on an existing file × access × every share value: legal options truncate to zero at open (same inode).</summary>
    [Theory, Catalog("OPEN-003", "U", "INV-02", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_003_CreateExistingTruncatesAtOpen(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"Create on existing files with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open003");
            foreach (FileShare share in OpenOracle.AllShares)
                await matrix.RunAsync(FileMode.Create, access, share, exists: true, held: []);
            matrix.AssertAllPassed(OpenOracle.AllShares.Count,
                OpenOracle.IsWritable(access) ? [OpenOutcome.Truncated, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>Create on a file with active handles: incompatible opens fail with a typed sharing violation and do NOT truncate.</summary>
    [Theory, Catalog("OPEN-003", "U", "INV-02", "INV-07")]
    [MemberData(nameof(WritableAccessValues))]
    public async Task OPEN_003_CreateExistingTruncatesAtOpen_WithActiveHandles(string accessName, int accessValue)
    {
        await RunActiveHandleMatrixAsync("/open003h", FileMode.Create, (FileAccess)accessValue, accessName, OpenOutcome.Truncated, OpenOutcome.SharingViolation);
    }

    /// <summary>
    /// The truncation is a durable committed effect of the open itself: it is visible before any write and survives an
    /// abrupt crash of every node (only durable images are restarted), across inline/extent/multi-extent sizes.
    /// </summary>
    [Theory, Catalog("OPEN-003", "U", "INV-02", "INV-07")]
    [InlineData(1)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData(200_000)]
    public async Task OPEN_003_CreateExistingTruncatesAtOpen_DurableBeforeAnyWrite(int initialLength)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct);
            const string path = "/t";
            byte[] old = TestData.Bytes(0x3003, initialLength);
            {
                await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
                await setup.GetVolume(Vol).Files.WriteAllBytesAsync(path, old, Ct);
            }

            FileInspection before = Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path));
            DfsStream stream = await writer.GetVolume(Vol).Files.OpenAsync(path, FileMode.Create, FileAccess.Write, FileShare.Read, Ct);
            Assert.Equal(before.Inode, stream.InodeId);

            FileInspection afterOpen = Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path));
            Assert.Equal(before.Inode, afterOpen.Inode);
            Assert.Equal(0L, afterOpen.Length);
            Assert.Empty(afterOpen.Extents);
            Assert.True(afterOpen.Revision > before.Revision, "truncation must be published by the open itself");

            // Abandon the writer without any write/flush/dispose, then crash every node and restart from durable images.
            cluster.KillClient(writer);
            await OpenHelpers.CrashAndRestartAllAsync(cluster);

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
            DfsFileInfo info = await reader.GetVolume(Vol).Files.GetInfoAsync(path, Ct);
            Assert.Equal(before.Inode, info.InodeId);
            Assert.Equal(0L, info.Length);
            Assert.Empty(await OpenHelpers.ReadSharedAsync(reader.GetVolume(Vol).Files, path)); // the killed writer's lease may still be live
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);
    }

    /// <summary>FSD §5.4: a later failed write does not restore the contents a successful Create already truncated.</summary>
    [Fact, Catalog("OPEN-003", "U", "INV-02", "INV-07")]
    public async Task OPEN_003_CreateExistingTruncatesAtOpen_LaterFailedWriteDoesNotRestore()
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            const string path = "/t";
            byte[] old = TestData.Bytes(0x3004, 50_000);
            await obs.WriteAllBytesAsync(path, old, Ct);
            InodeId inode = (await obs.GetInfoAsync(path, Ct)).InodeId;

            DfsStream stream = await client.GetVolume(Vol).Files.OpenAsync(path, FileMode.Create, FileAccess.Write, FileShare.Read, Ct);
            Assert.Equal(0L, (await obs.GetInfoAsync(path, Ct)).Length);

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Volume: Vol, Count: 100_000), FaultAction.IoError());
            await stream.WriteAsync(TestData.Bytes(0x3005, 20_000), Ct); // above the inline threshold: needs object writes
            Exception? flushError = await Record.ExceptionAsync(() => stream.FlushAsync(Ct));
            cluster.Faults.AssertReached(arm);
            arm.Disarm();
            Assert.NotNull(flushError);
            Assert.True(flushError is IOException or TimeoutException, "flush failure must surface as a typed I/O/availability failure: " + OpenHelpers.Describe(flushError));

            DfsFileInfo during = await obs.GetInfoAsync(path, Ct);
            Assert.Equal(inode, during.InodeId);
            Assert.Equal(0L, during.Length);

            Exception? disposeError = await Record.ExceptionAsync(() => stream.DisposeAsync().AsTask());
            OpenHelpers.Log("dispose after failed flush: " + OpenHelpers.Describe(disposeError));
            byte[] final = await obs.ReadAllBytesAsync(path, Ct);
            Assert.NotEqual(old.Length, final.Length);
            Assert.True(final.Length == 0 || final.Length == 20_000,
                $"only the truncated state or the complete new write may be visible, never the old contents or a partial write (length {final.Length})");
            if (final.Length == 20_000)
                AssertEx.BytesEqual(TestData.Bytes(0x3005, 20_000), final, "a retried flush/dispose may only publish the complete accepted write");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-004 CreateMissingCreates
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Create on a missing name × access × every share value.</summary>
    [Theory, Catalog("OPEN-004", "U", "INV-02", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_004_CreateMissingCreates(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"Create on missing names with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open004");
            foreach (FileShare share in OpenOracle.AllShares)
                await matrix.RunAsync(FileMode.Create, access, share, exists: false, held: []);
            matrix.AssertAllPassed(OpenOracle.AllShares.Count,
                OpenOracle.IsWritable(access) ? [OpenOutcome.Created, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>
    /// Creation obeys namespace permissions for every creating mode: a principal without Write on the volume cannot create
    /// (UnauthorizedAccessException, nothing created) — including OpenOrCreate with read access — but may open an
    /// existing file for reading; a principal with Write creates and owns the file.
    /// </summary>
    [Theory, Catalog("OPEN-004", "U", "INV-07", "INV-12")]
    [InlineData((int)FileMode.CreateNew)]
    [InlineData((int)FileMode.Create)]
    [InlineData((int)FileMode.OpenOrCreate)]
    [InlineData((int)FileMode.Append)]
    public async Task OPEN_004_CreateMissingCreates_ObeysNamespacePermissions(int modeValue)
    {
        FileMode mode = (FileMode)modeValue;
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            TestPrincipal readOnly = await cluster.CreatePrincipalAsync("ro", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read });
            TestPrincipal readWrite = await cluster.CreatePrincipalAsync("rw", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write });
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin" }, Ct);
            await using DfsClient ro = await cluster.ConnectAsync(new TestClientOptions { Name = "ro", Credentials = readOnly.Credentials }, Ct);
            await using DfsClient rw = await cluster.ConnectAsync(new TestClientOptions { Name = "rw", Credentials = readWrite.Credentials }, Ct);
            DfsFileClient adminFiles = admin.GetVolume(Vol).Files;
            await admin.GetVolume(Vol).Directories.CreateDirectoryAsync("/perm", Ct);
            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;

            var deniedAccesses = mode == FileMode.OpenOrCreate ? new[] { FileAccess.Write, FileAccess.Read, FileAccess.ReadWrite } : [FileAccess.Write];
            foreach (FileAccess access in deniedAccesses)
            {
                string path = "/perm/denied-" + access;
                FileShare share = access == FileAccess.Read ? FileShare.Read : FileShare.None;
                await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => ro.GetVolume(Vol).Files.OpenAsync(path, mode, access, share, Ct));
                Assert.False(await adminFiles.ExistsAsync(path, Ct), $"{mode}/{access}: a denied create left an entry");
                Assert.Null(await cluster.Inspect.GetFileAsync(Vol, path));
            }

            Assert.Equal(inodesBefore, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);

            // Opening an existing file needs no namespace permission: OpenOrCreate+Read by the read-only principal succeeds.
            if (mode == FileMode.OpenOrCreate)
            {
                byte[] content = TestData.Bytes(0x4004, 321);
                await adminFiles.WriteAllBytesAsync("/perm/existing", content, Ct);
                await using DfsStream readHandle = await ro.GetVolume(Vol).Files.OpenAsync("/perm/existing", mode, FileAccess.Read, FileShare.Read, Ct);
                await OpenHelpers.AssertBytesAsync(content, OpenHelpers.ReadToEndAsync(readHandle), "OpenOrCreate on an existing file by a read-only principal");
            }

            FileAccess createAccess = mode == FileMode.Append ? FileAccess.Write : FileAccess.ReadWrite;
            await using (DfsStream created = await rw.GetVolume(Vol).Files.OpenAsync("/perm/allowed", mode, createAccess, FileShare.Read, Ct))
            {
                DfsFileInfo info = await adminFiles.GetInfoAsync("/perm/allowed", Ct);
                Assert.Equal(created.InodeId, info.InodeId);
                Assert.Equal(0L, info.Length);
                Assert.Equal("rw", info.Owner);
            }

            Assert.True(await adminFiles.ExistsAsync("/perm/allowed", Ct));
        }, Ct);
    }

    /// <summary>Creation requires an existing parent directory; a missing parent or a file parent fails without creating anything.</summary>
    [Fact, Catalog("OPEN-004", "U", "INV-02", "INV-07")]
    public async Task OPEN_004_CreateMissingCreates_RequiresParentDirectory()
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsDirectoryClient dirs = client.GetVolume(Vol).Directories;
            byte[] parentFileContent = TestData.Bytes(0x4005, 777);
            await files.WriteAllBytesAsync("/pf", parentFileContent, Ct);
            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;

            foreach (FileMode mode in new[] { FileMode.CreateNew, FileMode.Create, FileMode.OpenOrCreate, FileMode.Append })
            {
                await Assert.ThrowsAsync<DirectoryNotFoundException>(() => files.OpenAsync("/nope/f", mode, FileAccess.Write, FileShare.None, Ct));
                Assert.False(await dirs.ExistsAsync("/nope", Ct), $"{mode}: a creating open must not create missing parents");

                Exception? underFile = await Record.ExceptionAsync(() => files.OpenAsync("/pf/x", mode, FileAccess.Write, FileShare.None, Ct));
                Assert.True(underFile is DirectoryNotFoundException or DfsException { ErrorCode: DfsErrorCode.NotADirectory },
                    $"{mode}: creating below a file must fail as DirectoryNotFound or NotADirectory, got {OpenHelpers.Describe(underFile)}");
            }

            await OpenHelpers.AssertBytesAsync(parentFileContent, files.ReadAllBytesAsync("/pf", Ct), "the file used as a parent is unchanged");
            Assert.Equal(inodesBefore, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);
        }, Ct);
    }

    /// <summary>
    /// A created file inherits the volume default policy unless a named policy is requested; the policy is recorded on
    /// the inode and its first durable publication meets that policy's acknowledgement domains. Unknown names fail.
    /// </summary>
    [Fact, Catalog("OPEN-004", "U", "INV-02", "INV-07")]
    public async Task OPEN_004_CreateMissingCreates_InheritsStoragePolicy()
    {
        var pair = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 };
        TestClusterOptions options = OpenHelpers.UnitOptions with
        {
            Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["pair"] = pair } }],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsVolumeInfo volume = await client.GetVolume(Vol).GetInfoAsync(Ct);
            await files.WriteAllBytesAsync("/ref", TestData.Bytes(1, 10), Ct);
            string defaultPolicy = (await files.GetInfoAsync("/ref", Ct)).StoragePolicy;
            Assert.NotEqual("pair", defaultPolicy);

            byte[] payload = TestData.Bytes(0x4006, 150_000);
            foreach ((string path, string? requested, DfsStoragePolicy effective) in new[]
                     { ("/inherit", (string?)null, volume.DefaultPolicy), ("/named", "pair", pair) })
            {
                await using (DfsStream s = await files.OpenAsync(path, new DfsOpenOptions
                             { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None, StoragePolicy = requested }, Ct))
                {
                    DfsFileInfo created = await files.GetInfoAsync(path, Ct);
                    Assert.Equal(requested ?? defaultPolicy, created.StoragePolicy);
                    Assert.Equal(requested ?? defaultPolicy, Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path)).Policy);
                    await s.WriteAsync(payload, Ct);
                    await s.FlushAsync(Ct);
                }

                FileInspection file = Assert.IsType<FileInspection>(await cluster.Inspect.GetFileAsync(Vol, path));
                Assert.NotEmpty(file.Extents);
                foreach (ExtentInspection extent in file.Extents)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
                    Assert.True(copies >= effective.DurableCopiesBeforeAck && zones >= effective.DurableZonesBeforeAck,
                        $"{path}: object {extent.Object} acknowledged with {copies} copies/{zones} zones, policy requires {effective.DurableCopiesBeforeAck}/{effective.DurableZonesBeforeAck}");
                }

                await OpenHelpers.AssertBytesAsync(payload, files.ReadAllBytesAsync(path, Ct));
            }

            Exception? unknown = await Record.ExceptionAsync(() => files.OpenAsync("/unknown-policy",
                new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None, StoragePolicy = "no-such-policy" }, Ct));
            Assert.True(unknown is ArgumentException or DfsException { ErrorCode: DfsErrorCode.InvalidPolicy },
                "an unknown named policy must be rejected explicitly, got " + OpenHelpers.Describe(unknown));
            Assert.False(await files.ExistsAsync("/unknown-policy", Ct));
        }, Ct);
    }

    /// <summary>
    /// Creation needs the metadata majority: with the volume leader cut off from both followers a creating open never
    /// reports success; after healing, the authoritative outcome of its operation id is definite and matches the
    /// namespace, and a definite failure was never actually committed.
    /// </summary>
    [Fact, Catalog("OPEN-004", "U", "INV-02", "INV-07")]
    public async Task OPEN_004_CreateMissingCreates_RequiresMetadataMajority()
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            const string path = "/majority";
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);
            foreach (TestNode node in cluster.Nodes.Where(n => n.Name != leader.Name))
                cluster.Network.Isolate(node.Name);

            Exception? error;
            DfsStream? unexpected = null;
            try
            {
                unexpected = await volume.Files.OpenAsync(path, new DfsOpenOptions
                    { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None, OperationId = id }, Ct);
                error = null;
            }
            catch (Exception ex)
            {
                error = ex;
            }

            Assert.Null(unexpected); // success without a majority would be a fabricated durable create
            Assert.True(error is DfsUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException,
                "expected an availability failure or an unknown outcome, got " + OpenHelpers.Describe(error));
            if (error is DfsCommitOutcomeUnknownException unknown)
                Assert.Equal(id, unknown.OperationId);

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
            Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Committed, DfsOutcomeStatus.Aborted });
            if (error is not DfsCommitOutcomeUnknownException)
                Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status); // a definite failure must not have committed (FSD §13)

            bool exists = await volume.Files.ExistsAsync(path, Ct);
            Assert.Equal(outcome.Status == DfsOutcomeStatus.Committed, exists);
            if (exists)
                Assert.Equal(0L, (await volume.Files.GetInfoAsync(path, Ct)).Length);
        }, Ct);
    }

    /// <summary>A file created by an open (no writes) is durable: it survives an abrupt crash and restart of every node.</summary>
    [Fact, Catalog("OPEN-004", "U", "INV-02", "INV-07")]
    public async Task OPEN_004_CreateMissingCreates_SurvivesCrashOfEveryNode()
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            DfsClient creator = await cluster.ConnectAsync(new TestClientOptions { Name = "creator" }, Ct);
            DfsStream stream = await creator.GetVolume(Vol).Files.OpenAsync("/durable", FileMode.Create, FileAccess.Write, FileShare.Read, Ct);
            InodeId inode = stream.InodeId;
            cluster.KillClient(creator);
            await OpenHelpers.CrashAndRestartAllAsync(cluster);

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
            DfsFileInfo info = await reader.GetVolume(Vol).Files.GetInfoAsync("/durable", Ct);
            Assert.Equal(inode, info.InodeId);
            Assert.Equal(0L, info.Length);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-005 OpenMissingFails
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Open on a missing name × access × every share value: FileNotFoundException (or option rejection), nothing created.</summary>
    [Theory, Catalog("OPEN-005", "U", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_005_OpenMissingFails(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"Open on missing names with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open005");
            foreach (FileShare share in OpenOracle.AllShares)
                await matrix.RunAsync(FileMode.Open, access, share, exists: false, held: []);
            matrix.AssertAllPassed(OpenOracle.AllShares.Count,
                OpenOracle.IsDefinedAccess(access) ? [OpenOutcome.NotFound, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>
    /// Deleted names, names below a missing directory and recreated names: Open/Truncate of the absent name fail as
    /// missing without creating anything; a recreated name opens the new inode, never the deleted one.
    /// </summary>
    [Fact, Catalog("OPEN-005", "U", "INV-07")]
    public async Task OPEN_005_OpenMissingFails_DeletedAndRecreatedNames()
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsDirectoryClient dirs = client.GetVolume(Vol).Directories;
            await dirs.CreateDirectoryAsync("/n", Ct);

            await files.WriteAllBytesAsync("/n/deleted", TestData.Bytes(1, 100), Ct);
            await files.DeleteAsync("/n/deleted", Ct);
            int inodes = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;
            foreach (FileAccess access in OpenOracle.ValidAccesses)
            {
                FileShare share = access == FileAccess.Read ? FileShare.ReadWrite : FileShare.Read;
                await Assert.ThrowsAsync<FileNotFoundException>(() => files.OpenAsync("/n/deleted", FileMode.Open, access, share, Ct));
            }

            await Assert.ThrowsAsync<FileNotFoundException>(() => files.OpenReadAsync("/n/deleted", Ct));
            await Assert.ThrowsAsync<FileNotFoundException>(() => files.OpenAsync("/n/deleted", FileMode.Truncate, FileAccess.Write, FileShare.None, Ct));
            Assert.Throws<FileNotFoundException>(() => files.Open("/n/deleted", FileMode.Open, FileAccess.Read, FileShare.Read));
            Assert.False(await files.ExistsAsync("/n/deleted", Ct));
            Assert.Equal(0, (await dirs.GetInfoAsync("/n", Ct)).EntryCount);
            Assert.Equal(inodes, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);

            Exception? missingParent = await Record.ExceptionAsync(() => files.OpenAsync("/no-dir/f", FileMode.Open, FileAccess.Read, FileShare.Read, Ct));
            Assert.True(missingParent is FileNotFoundException or DirectoryNotFoundException,
                "a name below a missing directory must fail as missing: " + OpenHelpers.Describe(missingParent));
            Assert.False(await dirs.ExistsAsync("/no-dir", Ct));

            await files.WriteAllBytesAsync("/n/re", TestData.Bytes(2, 300), Ct);
            InodeId first = (await files.GetInfoAsync("/n/re", Ct)).InodeId;
            await files.DeleteAsync("/n/re", Ct);
            await Assert.ThrowsAsync<FileNotFoundException>(() => files.OpenReadAsync("/n/re", Ct));
            byte[] second = TestData.Bytes(3, 301);
            await files.WriteAllBytesAsync("/n/re", second, Ct);
            await using DfsStream reopened = await files.OpenReadAsync("/n/re", Ct);
            Assert.NotEqual(first, reopened.InodeId);
            await OpenHelpers.AssertBytesAsync(second, OpenHelpers.ReadToEndAsync(reopened), "a recreated name opens the new inode");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-006 OpenExistingPreservesBytes
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Open on an existing file × access × every share value: opening never changes data, length or inode.</summary>
    [Theory, Catalog("OPEN-006", "U", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_006_OpenExistingPreservesBytes(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"Open on existing files with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open006");
            foreach (FileShare share in OpenOracle.AllShares)
                await matrix.RunAsync(FileMode.Open, access, share, exists: true, held: []);
            matrix.AssertAllPassed(OpenOracle.AllShares.Count,
                OpenOracle.IsDefinedAccess(access) ? [OpenOutcome.Opened, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>Open with active handles: share compatibility in both directions across all handles; conflicts change nothing.</summary>
    [Theory, Catalog("OPEN-006", "U", "INV-05", "INV-07")]
    [MemberData(nameof(EveryDefinedAccessValue))]
    public async Task OPEN_006_OpenExistingPreservesBytes_WithActiveHandles(string accessName, int accessValue)
    {
        await RunActiveHandleMatrixAsync("/open006h", FileMode.Open, (FileAccess)accessValue, accessName, OpenOutcome.Opened, OpenOutcome.SharingViolation);
    }

    /// <summary>
    /// Merely opening (Open, OpenOrCreate, Append; read, write and read/write access) does not change bytes, length or
    /// inode, for every boundary length around the inline threshold, chunk and buffer sizes.
    /// </summary>
    [Theory, Catalog("OPEN-006", "U", "INV-07")]
    [MemberData(nameof(BoundaryLengths))]
    public async Task OPEN_006_OpenExistingPreservesBytes_AcrossLengths(long length)
    {
        (FileMode Mode, FileAccess Access, FileShare Share)[] variants =
        [
            (FileMode.Open, FileAccess.Read, FileShare.ReadWrite), (FileMode.Open, FileAccess.Write, FileShare.Read),
            (FileMode.Open, FileAccess.ReadWrite, FileShare.None), (FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read),
            (FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete), (FileMode.Append, FileAccess.Write, FileShare.Read),
        ];
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            byte[] content = TestData.Bytes(0x6006 + (ulong)length, checked((int)length));
            await obs.WriteAllBytesAsync("/f", content, Ct);
            DfsFileInfo original = await obs.GetInfoAsync("/f", Ct);

            foreach ((FileMode mode, FileAccess access, FileShare share) in variants)
            {
                string what = $"{mode}/{access}/{share} at length {length}";
                DfsFileInfo before = await obs.GetInfoAsync("/f", Ct);
                await using (DfsStream s = await files.OpenAsync("/f", mode, access, share, Ct))
                {
                    Assert.Equal(original.InodeId, s.InodeId);
                    if (mode != FileMode.Append)
                    {
                        Assert.Equal(length, await s.GetLengthAsync(Ct));
                        Assert.Equal(0L, s.Position);
                    }

                    if (s.CanRead)
                        await OpenHelpers.AssertBytesAsync(content, OpenHelpers.ReadToEndAsync(s), what + ": bytes read through the handle");
                    DfsFileInfo during = await obs.GetInfoAsync("/f", Ct);
                    Assert.Equal(length, during.Length);
                    if (access == FileAccess.Read)
                        Assert.Equal(before.Revision, during.Revision);
                }

                DfsFileInfo after = await obs.GetInfoAsync("/f", Ct);
                Assert.Equal(original.InodeId, after.InodeId);
                Assert.Equal(length, after.Length);
                await OpenHelpers.AssertBytesAsync(content, obs.ReadAllBytesAsync("/f", Ct), what + ": committed bytes after closing");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-007 is level S.
    // OPEN-008 TruncateRequiresExistingWriteOnly
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Truncate × every access × every share value × missing/existing: only write-only access on an existing file with
    /// legal sharing succeeds (and truncates at open); missing → FileNotFoundException; read/read-write → rejected.
    /// </summary>
    [Theory, Catalog("OPEN-008", "U", "INV-02", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_008_TruncateRequiresExistingWriteOnly(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"Truncate with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open008");
            foreach (FileShare share in OpenOracle.AllShares)
            {
                foreach (bool exists in MissingThenExisting)
                    await matrix.RunAsync(FileMode.Truncate, access, share, exists, held: []);
            }

            matrix.AssertAllPassed(OpenOracle.AllShares.Count * 2,
                access == FileAccess.Write ? [OpenOutcome.Truncated, OpenOutcome.NotFound, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>Truncate with active handles: a conflicting truncate fails with a typed sharing violation and does not truncate.</summary>
    [Fact, Catalog("OPEN-008", "U", "INV-02", "INV-07")]
    public async Task OPEN_008_TruncateRequiresExistingWriteOnly_WithActiveHandles()
    {
        await RunActiveHandleMatrixAsync("/open008h", FileMode.Truncate, FileAccess.Write, "Write", OpenOutcome.Truncated, OpenOutcome.SharingViolation);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-009 OpenWriteDoesNotTruncate
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// OpenWrite (OpenOrCreate, Write, Share.Read, offset 0) on a longer file followed by a short prefix overwrite: the old
    /// suffix survives; the open itself does not shorten the file. -1 = missing file (OpenWrite creates it).
    /// </summary>
    [Theory, Catalog("OPEN-009", "U", "INV-07")]
    [InlineData(10, 1, false)]
    [InlineData(10, 9, true)]
    [InlineData(4097, 100, false)]
    [InlineData(4097, 4096, true)]
    [InlineData(70_000, 65_536, false)]
    [InlineData(70_000, 1, true)]
    [InlineData(200_000, 70_000, false)]
    [InlineData(-1, 100, false)]
    public async Task OPEN_009_OpenWriteDoesNotTruncate(int fileLength, int prefixLength, bool writeThrough)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            const string path = "/ow";
            byte[] old = fileLength < 0 ? [] : TestData.Bytes(0x9009, fileLength);
            InodeId? inode = null;
            if (fileLength >= 0)
            {
                await obs.WriteAllBytesAsync(path, old, Ct);
                inode = (await obs.GetInfoAsync(path, Ct)).InodeId;
            }

            byte[] prefix = TestData.Bytes(0x900A, prefixLength);
            byte[] expected = OpenHelpers.Overlay(old, prefix);
            DfsStream s = writeThrough
                ? await files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough }, Ct)
                : await files.OpenWriteAsync(path, Ct);
            await using (s)
            {
                Assert.Equal(FileAccess.Write, s.Access);
                Assert.Equal(FileShare.Read, s.Share);
                Assert.True(s.CanSeek);
                Assert.False(s.IsAppend);
                Assert.Equal(0L, s.Position);
                Assert.Equal(old.LongLength, await s.GetLengthAsync(Ct));
                if (inode is not null)
                    Assert.Equal(inode.Value, s.InodeId);
                Assert.Equal(old.LongLength, (await obs.GetInfoAsync(path, Ct)).Length); // the open did not truncate

                await s.WriteAsync(prefix, Ct);
                Assert.Equal(prefix.LongLength, s.Position);
                Assert.Equal(expected.LongLength, await s.GetLengthAsync(Ct));
                if (writeThrough)
                    await OpenHelpers.AssertBytesAsync(expected, OpenHelpers.ReadSharedAsync(obs, path), "write-through publishes the prefix overwrite on return");
            }

            DfsFileInfo info = await obs.GetInfoAsync(path, Ct);
            if (inode is not null)
                Assert.Equal(inode.Value, info.InodeId);
            Assert.Equal(expected.LongLength, info.Length);
            await OpenHelpers.AssertBytesAsync(expected, obs.ReadAllBytesAsync(path, Ct), "prefix overwritten, old suffix intact");
        }, Ct);
    }

    /// <summary>OpenWrite + explicit SetLength: only SetLength changes the length (shrink or zero-extend); Position is unchanged.</summary>
    [Theory, Catalog("OPEN-009", "U", "INV-07")]
    [InlineData(5000, 10, 3000)]
    [InlineData(5000, 10, 5)]
    [InlineData(5000, 10, 9000)]
    [InlineData(100_000, 70_000, 65_536)]
    public async Task OPEN_009_OpenWriteDoesNotTruncate_UnlessSetLength(int fileLength, int prefixLength, int newLength)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] old = TestData.Bytes(0x900B, fileLength);
            await files.WriteAllBytesAsync("/sl", old, Ct);
            byte[] prefix = TestData.Bytes(0x900C, prefixLength);
            byte[] expected = OpenHelpers.Overlay(old, prefix);
            Array.Resize(ref expected, newLength); // shrink, or extend with zeroes

            await using (DfsStream s = await files.OpenWriteAsync("/sl", Ct))
            {
                await s.WriteAsync(prefix, Ct);
                s.SetLength(newLength);
                Assert.Equal(prefixLength, s.Position); // FSD §6.2: SetLength never moves Position
                Assert.Equal(newLength, await s.GetLengthAsync(Ct));
            }

            await OpenHelpers.AssertBytesAsync(expected, files.ReadAllBytesAsync("/sl", Ct), "only the explicit SetLength changed the length");
        }, Ct);
    }

    /// <summary>OpenOrCreate × access × every share value × missing/existing (the mode behind OpenWrite).</summary>
    [Theory, Catalog("OPEN-009", "U", "INV-02", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_009_OpenWriteDoesNotTruncate_OpenOrCreateMatrix(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"OpenOrCreate with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open009");
            foreach (FileShare share in OpenOracle.AllShares)
            {
                foreach (bool exists in MissingThenExisting)
                    await matrix.RunAsync(FileMode.OpenOrCreate, access, share, exists, held: []);
            }

            matrix.AssertAllPassed(OpenOracle.AllShares.Count * 2,
                OpenOracle.IsDefinedAccess(access) ? [OpenOutcome.Created, OpenOutcome.Opened, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    /// <summary>OpenOrCreate on an existing file with active handles.</summary>
    [Theory, Catalog("OPEN-009", "U", "INV-05", "INV-07")]
    [MemberData(nameof(EveryDefinedAccessValue))]
    public async Task OPEN_009_OpenWriteDoesNotTruncate_WithActiveHandles(string accessName, int accessValue)
    {
        await RunActiveHandleMatrixAsync("/open009h", FileMode.OpenOrCreate, (FileAccess)accessValue, accessName, OpenOutcome.Opened, OpenOutcome.SharingViolation);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-010 AppendTargetsLogicalEnd
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Append handles cannot seek; every write (array, memory, byte and span overloads; OnFlush and WriteThrough) lands at
    /// the staged logical end and the original prefix stays intact. -1 = missing file (Append creates it).
    /// </summary>
    [Theory, Catalog("OPEN-010", "U", "INV-02", "INV-07")]
    [InlineData(-1, false)]
    [InlineData(-1, true)]
    [InlineData(0, false)]
    [InlineData(0, true)]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(4095, false)]
    [InlineData(4095, true)]
    [InlineData(4097, false)]
    [InlineData(4097, true)]
    [InlineData(70_000, false)]
    [InlineData(70_000, true)]
    public async Task OPEN_010_AppendTargetsLogicalEnd(int prefixLength, bool writeThrough)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            const string path = "/log";
            byte[] prefix = prefixLength < 0 ? [] : TestData.Bytes(0xA010, prefixLength);
            InodeId? inode = null;
            if (prefixLength >= 0)
            {
                await obs.WriteAllBytesAsync(path, prefix, Ct);
                inode = (await obs.GetInfoAsync(path, Ct)).InodeId;
            }

            byte[] s1 = TestData.Bytes(0xA011, 3000), s2 = TestData.Bytes(0xA012, 70_000), s4 = TestData.Bytes(0xA014, 17), s5 = TestData.Bytes(0xA015, 4097);
            const byte s3 = 0x5A;
            var expected = new List<byte>(prefix);

            DfsStream s = await files.OpenAsync(path, new DfsOpenOptions
            {
                Mode = FileMode.Append, Access = FileAccess.Write, Share = FileShare.Read,
                WriteMode = writeThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush,
            }, Ct);
            await using (s)
            {
                Assert.True(s.IsAppend);
                Assert.False(s.CanSeek);
                Assert.False(s.CanRead);
                Assert.True(s.CanWrite);
                if (inode is not null)
                    Assert.Equal(inode.Value, s.InodeId);

                async Task AfterWriteAsync(string step)
                {
                    DfsFileInfo info = await obs.GetInfoAsync(path, Ct);
                    if (writeThrough)
                        Assert.Equal(expected.Count, info.Length); // published at the logical end on return
                    else
                        Assert.Equal(prefix.LongLength, info.Length); // staged only; the original prefix is all others see
                    OpenHelpers.Log($"{step}: visible length {info.Length}");
                }

                s.Write(s1, 0, s1.Length);
                expected.AddRange(s1);
                await AfterWriteAsync("Write(byte[])");
                await s.WriteAsync(s2.AsMemory(), Ct);
                expected.AddRange(s2);
                await AfterWriteAsync("WriteAsync(memory)");
                s.WriteByte(s3);
                expected.Add(s3);
                await AfterWriteAsync("WriteByte");
                s.Write(s4.AsSpan());
                expected.AddRange(s4);
                await AfterWriteAsync("Write(span)");

                await s.FlushAsync(Ct);
                await OpenHelpers.AssertBytesAsync(expected.ToArray(), OpenHelpers.ReadSharedAsync(obs, path), "after flush: prefix + appended segments in order");

                await s.WriteAsync(s5, 0, s5.Length, Ct);
                expected.AddRange(s5);
            }

            DfsFileInfo final = await obs.GetInfoAsync(path, Ct);
            if (inode is not null)
                Assert.Equal(inode.Value, final.InodeId);
            byte[] bytes = await obs.ReadAllBytesAsync(path, Ct);
            AssertEx.BytesEqual(expected.ToArray(), bytes, "after dispose: every append landed at the logical end");
            AssertEx.BytesEqual(prefix, bytes.AsSpan(0, prefix.Length), "the original prefix is intact");
        }, Ct);
    }

    /// <summary>Append with active handles: one writer per inode; readers must share Write; conflicts change nothing.</summary>
    [Fact, Catalog("OPEN-010", "U", "INV-05", "INV-07")]
    public async Task OPEN_010_AppendTargetsLogicalEnd_WithActiveHandles()
    {
        await RunActiveHandleMatrixAsync("/open010h", FileMode.Append, FileAccess.Write, "Write", OpenOutcome.Opened, OpenOutcome.SharingViolation);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-011 AppendRejectsReadAndSetLength
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// On an append handle every seek, Position assignment, SetLength and read is NotSupportedException; neither the
    /// committed contents nor the handle's staged appends change, and the handle stays usable.
    /// </summary>
    [Theory, Catalog("OPEN-011", "U", "INV-07")]
    [InlineData("Seek(0,Begin)")]
    [InlineData("Seek(0,Current)")]
    [InlineData("Seek(0,End)")]
    [InlineData("Position=0")]
    [InlineData("Position=end")]
    [InlineData("SetLength(shorter)")]
    [InlineData("SetLength(longer)")]
    [InlineData("SetLengthAsync")]
    [InlineData("Read(byte[])")]
    [InlineData("ReadAsync(memory)")]
    [InlineData("ReadByte")]
    public async Task OPEN_011_AppendRejectsReadAndSetLength(string operation)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            const string path = "/app";
            byte[] prefix = TestData.Bytes(0xB011, 5000);
            await obs.WriteAllBytesAsync(path, prefix, Ct);
            DfsFileInfo before = await obs.GetInfoAsync(path, Ct);
            byte[] staged = TestData.Bytes(0xB012, 700), after = TestData.Bytes(0xB013, 900);

            await using (DfsStream s = await client.GetVolume(Vol).Files.OpenAsync(path, FileMode.Append, FileAccess.Write, FileShare.Read, Ct))
            {
                await s.WriteAsync(staged, Ct); // pending, unflushed append
                long end = prefix.Length + staged.Length;
                Func<Task> act = operation switch
                {
                    "Seek(0,Begin)" => () => Sync(() => s.Seek(0, SeekOrigin.Begin)),
                    "Seek(0,Current)" => () => Sync(() => s.Seek(0, SeekOrigin.Current)),
                    "Seek(0,End)" => () => Sync(() => s.Seek(0, SeekOrigin.End)),
                    "Position=0" => () => Sync(() => s.Position = 0),
                    "Position=end" => () => Sync(() => s.Position = end),
                    "SetLength(shorter)" => () => Sync(() => s.SetLength(10)),
                    "SetLength(longer)" => () => Sync(() => s.SetLength(end + 100)),
                    "SetLengthAsync" => () => s.SetLengthAsync(10, Ct),
                    "Read(byte[])" => () => Sync(() => s.Read(new byte[16], 0, 16)),
                    "ReadAsync(memory)" => () => s.ReadAsync(new byte[16].AsMemory(), Ct).AsTask(),
                    "ReadByte" => () => Sync(() => s.ReadByte()),
                    _ => throw new ArgumentOutOfRangeException(nameof(operation)),
                };
                await Assert.ThrowsAsync<NotSupportedException>(act);
                Assert.False(s.IsFaulted, "a rejected unsupported call must not fault the append handle");

                DfsFileInfo during = await obs.GetInfoAsync(path, Ct);
                Assert.Equal(before.Revision, during.Revision);
                await OpenHelpers.AssertBytesAsync(prefix, OpenHelpers.ReadSharedAsync(obs, path), "committed contents unchanged by the rejected " + operation);

                await s.WriteAsync(after, Ct);
            }

            await OpenHelpers.AssertBytesAsync(OpenHelpers.Concat(prefix, staged, after), obs.ReadAllBytesAsync(path, Ct),
                "staged appends survived the rejected operation and later appends still target the logical end");
        }, Ct);
    }

    /// <summary>Append × every access × every share value × missing/existing: only write-only access is accepted.</summary>
    [Theory, Catalog("OPEN-011", "U", "INV-07")]
    [MemberData(nameof(AllAccessValues))]
    public async Task OPEN_011_AppendRejectsReadAndSetLength_AccessMatrix(string accessName, int accessValue)
    {
        FileAccess access = (FileAccess)accessValue;
        OpenHelpers.Log($"Append with access {accessName}");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open011");
            foreach (FileShare share in OpenOracle.AllShares)
            {
                foreach (bool exists in MissingThenExisting)
                    await matrix.RunAsync(FileMode.Append, access, share, exists, held: []);
            }

            matrix.AssertAllPassed(OpenOracle.AllShares.Count * 2,
                access == FileAccess.Write ? [OpenOutcome.Created, OpenOutcome.Opened, OpenOutcome.Rejected] : [OpenOutcome.Rejected]);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-012 ReadOnlyDeniesMutation
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every mutating member (Write overloads, WriteByte, SetLength, SetLengthAsync, CommitAsync) fails on every kind of
    /// read-only handle; Flush is a no-op; reads still return the exact bytes and nothing is published.
    /// </summary>
    [Theory, Catalog("OPEN-012", "U", "INV-07", "INV-12")]
    [InlineData("Write(byte[])")]
    [InlineData("Write(span)")]
    [InlineData("WriteAsync(byte[])")]
    [InlineData("WriteAsync(memory)")]
    [InlineData("WriteByte")]
    [InlineData("SetLength")]
    [InlineData("SetLengthAsync")]
    [InlineData("CommitAsync")]
    [InlineData("CommitAsync(options)")]
    public async Task OPEN_012_ReadOnlyDeniesMutation(string mutation)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            await observer.GetVolume(Vol).Directories.CreateDirectoryAsync("/ro", Ct);
            byte[] content = TestData.Bytes(0xC012, 5000);
            byte[] data = TestData.Bytes(0xC013, 64);

            (string Name, Func<string, Task<DfsStream>> Open)[] variants =
            [
                ("OpenReadAsync", p => files.OpenReadAsync(p, Ct)),
                ("OpenRead (sync)", p => Task.FromResult(files.OpenRead(p))),
                ("Open Read/None", p => files.OpenAsync(p, FileMode.Open, FileAccess.Read, FileShare.None, Ct)),
                ("Open Read/Read", p => files.OpenAsync(p, FileMode.Open, FileAccess.Read, FileShare.Read, Ct)),
                ("Open Read/Write", p => files.OpenAsync(p, FileMode.Open, FileAccess.Read, FileShare.Write, Ct)),
                ("Open Read/ReadWrite+Delete", p => files.OpenAsync(p, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, Ct)),
                ("OpenOrCreate Read/Read (existing)", p => files.OpenAsync(p, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read, Ct)),
                ("Open Read/ReadWrite VersionPinned", p => files.OpenAsync(p, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite, ReadView = DfsReadView.VersionPinned }, Ct)),
            ];

            int index = 0;
            foreach ((string name, Func<string, Task<DfsStream>> open) in variants)
            {
                string path = "/ro/f" + index++;
                await obs.WriteAllBytesAsync(path, content, Ct);
                DfsFileInfo before = await obs.GetInfoAsync(path, Ct);
                await using DfsStream s = await open(path);
                Assert.True(s.CanRead, name);
                Assert.False(s.CanWrite, name);
                Assert.Equal(0UL, s.WriterGeneration);

                Func<Task> act = mutation switch
                {
                    "Write(byte[])" => () => Sync(() => s.Write(data, 0, data.Length)),
                    "Write(span)" => () => Sync(() => s.Write(data.AsSpan())),
                    "WriteAsync(byte[])" => () => s.WriteAsync(data, 0, data.Length, Ct),
                    "WriteAsync(memory)" => () => s.WriteAsync(data.AsMemory(), Ct).AsTask(),
                    "WriteByte" => () => Sync(() => s.WriteByte(1)),
                    "SetLength" => () => Sync(() => s.SetLength(1)),
                    "SetLengthAsync" => () => s.SetLengthAsync(1, Ct),
                    "CommitAsync" => () => s.CommitAsync(Ct),
                    "CommitAsync(options)" => () => s.CommitAsync(new DfsMutationOptions(), Ct),
                    _ => throw new ArgumentOutOfRangeException(nameof(mutation)),
                };
                Exception? denied = await Record.ExceptionAsync(act);
                Assert.True(mutation.StartsWith("Commit", StringComparison.Ordinal)
                        ? denied is NotSupportedException or InvalidOperationException
                        : denied is NotSupportedException,
                    $"{name}: {mutation} on a read-only handle must be rejected explicitly, got {OpenHelpers.Describe(denied)}");

                s.Flush();                 // read-only flush is a no-op (FSD §6.4)
                await s.FlushAsync(Ct);
                s.Position = 0;
                await OpenHelpers.AssertBytesAsync(content, OpenHelpers.ReadToEndAsync(s), $"{name}: reads still work after the denied {mutation}");

                DfsFileInfo after = await obs.GetInfoAsync(path, Ct);
                Assert.Equal(before.Revision, after.Revision);
                Assert.Equal(before.Length, after.Length);
                await s.DisposeAsync(); // some variants share nothing; compare committed bytes once the handle is closed
                await OpenHelpers.AssertBytesAsync(content, obs.ReadAllBytesAsync(path, Ct), $"{name}: committed contents unchanged");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-013 WriteOnlyDeniesRead
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every read member and reading helper fails on every kind of write-only handle without faulting it; staged writes
    /// before and after the denied read are published with the mode's normal semantics.
    /// </summary>
    [Theory, Catalog("OPEN-013", "U", "INV-07", "INV-12")]
    [InlineData("Read(byte[])")]
    [InlineData("Read(span)")]
    [InlineData("ReadAsync(byte[])")]
    [InlineData("ReadAsync(memory)")]
    [InlineData("ReadByte")]
    [InlineData("CopyTo")]
    [InlineData("CopyToAsync")]
    [InlineData("StreamReader")]
    public async Task OPEN_013_WriteOnlyDeniesRead(string readOperation)
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            await observer.GetVolume(Vol).Directories.CreateDirectoryAsync("/wo", Ct);
            byte[] old = TestData.Bytes(0xD013, 5000);
            byte[] w1 = TestData.Bytes(0xD014, 1000), w2 = TestData.Bytes(0xD015, 1500);
            byte[] written = OpenHelpers.Concat(w1, w2);

            (string Name, bool Existing, Func<string, Task<DfsStream>> Open, byte[] Expected)[] variants =
            [
                ("Open Write/Read", true, p => files.OpenAsync(p, FileMode.Open, FileAccess.Write, FileShare.Read, Ct), OpenHelpers.Overlay(old, written)),
                ("OpenOrCreate Write/None", true, p => files.OpenAsync(p, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, Ct), OpenHelpers.Overlay(old, written)),
                ("OpenWriteAsync", true, p => files.OpenWriteAsync(p, Ct), OpenHelpers.Overlay(old, written)),
                ("Create Write/Read", true, p => files.OpenAsync(p, FileMode.Create, FileAccess.Write, FileShare.Read, Ct), written),
                ("CreateNew Write/Read", false, p => files.OpenAsync(p, FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct), written),
                ("Truncate Write/None", true, p => files.OpenAsync(p, FileMode.Truncate, FileAccess.Write, FileShare.None, Ct), written),
                ("Append Write/Read", true, p => files.OpenAsync(p, FileMode.Append, FileAccess.Write, FileShare.Read, Ct), OpenHelpers.Concat(old, written)),
            ];

            int index = 0;
            foreach ((string name, bool existing, Func<string, Task<DfsStream>> open, byte[] expected) in variants)
            {
                string path = "/wo/f" + index++;
                if (existing)
                    await obs.WriteAllBytesAsync(path, old, Ct);
                await using (DfsStream s = await open(path))
                {
                    Assert.False(s.CanRead, name);
                    Assert.True(s.CanWrite, name);
                    await s.WriteAsync(w1, Ct);

                    Func<Task> act = readOperation switch
                    {
                        "Read(byte[])" => () => Sync(() => s.Read(new byte[10], 0, 10)),
                        "Read(span)" => () => Sync(() => s.Read(new byte[10].AsSpan())),
                        "ReadAsync(byte[])" => () => s.ReadAsync(new byte[10], 0, 10, Ct),
                        "ReadAsync(memory)" => () => s.ReadAsync(new byte[10].AsMemory(), Ct).AsTask(),
                        "ReadByte" => () => Sync(() => s.ReadByte()),
                        "CopyTo" => () => Sync(() => s.CopyTo(new MemoryStream())),
                        "CopyToAsync" => () => s.CopyToAsync(new MemoryStream(), Ct),
                        "StreamReader" => () => Sync(() => new StreamReader(s, leaveOpen: true).ReadToEnd()),
                        _ => throw new ArgumentOutOfRangeException(nameof(readOperation)),
                    };
                    Exception? denied = await Record.ExceptionAsync(act);
                    Assert.True(readOperation == "StreamReader" ? denied is ArgumentException : denied is NotSupportedException,
                        $"{name}: {readOperation} on a write-only handle must be rejected explicitly, got {OpenHelpers.Describe(denied)}");
                    Assert.False(s.IsFaulted, $"{name}: a denied read must not fault the writable handle");

                    await s.WriteAsync(w2, Ct);
                    await s.FlushAsync(Ct);
                    DfsFileInfo flushed = await obs.GetInfoAsync(path, Ct); // metadata only: some variants share nothing with readers
                    Assert.True(expected.Length == flushed.Length, $"{name}: flush after the denied {readOperation} published length {flushed.Length}, expected {expected.Length}");
                }

                await OpenHelpers.AssertBytesAsync(expected, obs.ReadAllBytesAsync(path, Ct), $"{name}: contents after dispose");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-014..017 are level S.
    // OPEN-018 OpenInsideTransactionIsPrivate
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Creating/truncating/appending opens inside a transaction are private: other clients see the previous committed
    /// state (absence, old bytes, old revision) through open, flush and stream disposal; commit publishes exactly the
    /// transaction's view at its commit token; abort leaves no trace (no entry, no inode).
    /// </summary>
    [Theory, Catalog("OPEN-018", "U", "INV-02", "INV-07")]
    [InlineData((int)FileMode.CreateNew, false)]
    [InlineData((int)FileMode.Create, false)]
    [InlineData((int)FileMode.Create, true)]
    [InlineData((int)FileMode.Truncate, true)]
    [InlineData((int)FileMode.OpenOrCreate, false)]
    [InlineData((int)FileMode.Append, false)]
    [InlineData((int)FileMode.Append, true)]
    public async Task OPEN_018_OpenInsideTransactionIsPrivate(int modeValue, bool existing)
    {
        FileMode mode = (FileMode)modeValue;
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            DfsFileClient obs = observer.GetVolume(Vol).Files;
            await observer.GetVolume(Vol).Directories.CreateDirectoryAsync("/tx", Ct);

            foreach (bool commit in new[] { true, false })
            {
                string path = $"/tx/{mode}-{(commit ? "commit" : "abort")}";
                byte[] old = existing ? TestData.Bytes(0xE018, 6000) : [];
                byte[] data = TestData.Bytes(0xE019, 9000);
                byte[] expected = mode == FileMode.Append ? OpenHelpers.Concat(old, data) : data;
                DfsFileInfo? before = null;
                if (existing)
                {
                    await obs.WriteAllBytesAsync(path, old, Ct);
                    before = await obs.GetInfoAsync(path, Ct);
                }

                int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;

                async Task AssertOutsideUnchangedAsync(string stage)
                {
                    if (!existing)
                    {
                        Assert.False(await obs.ExistsAsync(path, Ct), $"{stage}: the transaction's create leaked to another client");
                        await Assert.ThrowsAsync<FileNotFoundException>(() => obs.GetInfoAsync(path, Ct));
                        await Assert.ThrowsAsync<FileNotFoundException>(() => obs.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct));
                        Assert.Null(await cluster.Inspect.GetFileAsync(Vol, path));
                        return;
                    }

                    DfsFileInfo now = await obs.GetInfoAsync(path, Ct);
                    Assert.Equal(before!.InodeId, now.InodeId);
                    Assert.Equal(before.Length, now.Length);
                    Assert.Equal(before.Revision, now.Revision);
                    await using DfsStream reader = await obs.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                    await OpenHelpers.AssertBytesAsync(old, OpenHelpers.ReadToEndAsync(reader), $"{stage}: other clients read the previous committed bytes");
                }

                await using (DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct))
                {
                    DfsStream s = await tx.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
                    Assert.True(s.IsTransactionBound);
                    await AssertOutsideUnchangedAsync("after the transactional open");

                    await s.WriteAsync(data, Ct);
                    await Assert.ThrowsAsync<InvalidOperationException>(() => s.CommitAsync(Ct)); // FSD §8.2
                    await s.FlushAsync(Ct);
                    await AssertOutsideUnchangedAsync("after flushing the bound stream");
                    await s.DisposeAsync();
                    await AssertOutsideUnchangedAsync("after disposing the bound stream");

                    Assert.True(await tx.Files.ExistsAsync(path, Ct));
                    await OpenHelpers.AssertBytesAsync(expected, tx.Files.ReadAllBytesAsync(path, Ct), "the transaction reads its own staged open/writes");

                    if (commit)
                    {
                        CommitToken token = await tx.CommitAsync(Ct);
                        Assert.False(token.IsNone);
                        DfsFileInfo info = await obs.GetInfoAsync(path, Ct);
                        Assert.Equal(token.Sequence, info.Revision);
                        if (existing)
                            Assert.Equal(before!.InodeId, info.InodeId);
                        await OpenHelpers.AssertBytesAsync(expected, obs.ReadAllBytesAsync(path, Ct), "committed transactional open + writes");
                    }
                    else
                    {
                        await tx.AbortAsync(Ct);
                        await AssertOutsideUnchangedAsync("after abort");
                        Assert.Equal(inodesBefore, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);
                    }
                }

                if (!commit)
                    await AssertOutsideUnchangedAsync("after disposing the aborted transaction");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OPEN-019 UnsupportedOsFeaturesFailExplicitly
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Inherited OS handles (FileShare.Inheritable) and unsupported FileOptions (WriteThrough, DeleteOnClose, Encrypted,
    /// undefined bits, mixtures with supported bits) throw NotSupportedException through every overload — they are never
    /// silently ignored — and nothing is created or changed.
    /// </summary>
    [Theory, Catalog("OPEN-019", "U", "INV-07", "INV-12")]
    [InlineData("FileShare.Inheritable", (int)FileShare.Inheritable, 0)]
    [InlineData("FileShare.Read|Inheritable", (int)(FileShare.Read | FileShare.Inheritable), 0)]
    [InlineData("FileOptions.WriteThrough", (int)FileShare.Read, unchecked((int)FileOptions.WriteThrough))]
    [InlineData("FileOptions.DeleteOnClose", (int)FileShare.Read, (int)FileOptions.DeleteOnClose)]
    [InlineData("FileOptions.Encrypted", (int)FileShare.Read, (int)FileOptions.Encrypted)]
    [InlineData("FileOptions.Undefined0x1", (int)FileShare.Read, 0x1)]
    [InlineData("FileOptions.Asynchronous|DeleteOnClose", (int)FileShare.Read, (int)(FileOptions.Asynchronous | FileOptions.DeleteOnClose))]
    [InlineData("FileOptions.SequentialScan|WriteThrough", (int)FileShare.Read, unchecked((int)(FileOptions.SequentialScan | FileOptions.WriteThrough)))]
    public async Task OPEN_019_UnsupportedOsFeaturesFailExplicitly(string feature, int shareValue, int optionsValue)
    {
        FileShare share = (FileShare)shareValue;
        FileOptions fileOptions = (FileOptions)optionsValue;
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] content = TestData.Bytes(0xF019, 5000);
            await files.WriteAllBytesAsync("/existing", content, Ct);
            DfsFileInfo before = await files.GetInfoAsync("/existing", Ct);
            long requestsBefore = OpenHelpers.CountOpenRequests(cluster, "c0");

            (string Path, FileMode Mode, FileAccess Access)[] attempts =
            [
                ("/missing", FileMode.CreateNew, FileAccess.Write), ("/missing", FileMode.OpenOrCreate, FileAccess.ReadWrite),
                ("/existing", FileMode.Open, FileAccess.Read), ("/existing", FileMode.Create, FileAccess.Write),
                ("/existing", FileMode.Append, FileAccess.Write),
            ];
            foreach ((string path, FileMode mode, FileAccess access) in attempts)
            {
                // A writable handle may not share Write; keep the share legal apart from the feature under test.
                FileShare effectiveShare = (access & FileAccess.Write) != 0 ? share & ~FileShare.Write : share;
                var options = new DfsOpenOptions { Mode = mode, Access = access, Share = effectiveShare, Options = fileOptions };
                string what = $"{feature} with {mode}/{access} on {path}";
                await Assert.ThrowsAsync<NotSupportedException>(() => files.OpenAsync(path, options, Ct));
                Assert.Throws<NotSupportedException>(() => files.Open(path, options));
                if (fileOptions == FileOptions.None)
                {
                    await Assert.ThrowsAsync<NotSupportedException>(() => files.OpenAsync(path, mode, access, effectiveShare, Ct));
                    Assert.Throws<NotSupportedException>(() => files.Open(path, mode, access, effectiveShare));
                }

                OpenHelpers.Log(what + ": NotSupportedException");
            }

            long requestsAfter = OpenHelpers.CountOpenRequests(cluster, "c0"); // before the verification reads below open handles
            Assert.False(await files.ExistsAsync("/missing", Ct));
            DfsFileInfo after = await files.GetInfoAsync("/existing", Ct);
            Assert.Equal(before.Revision, after.Revision);
            await OpenHelpers.AssertBytesAsync(content, files.ReadAllBytesAsync("/existing", Ct), "unsupported options must not truncate/append");
            Assert.Equal(requestsBefore, requestsAfter); // rejected before any request
        }, Ct);
    }

    /// <summary>The supported FileOptions (None, Asynchronous, SequentialScan, RandomAccess and combinations) are accepted and behave normally.</summary>
    [Fact, Catalog("OPEN-019", "U", "INV-07")]
    public async Task OPEN_019_UnsupportedOsFeaturesFailExplicitly_SupportedOptionsWork()
    {
        FileOptions[] supported =
        [
            FileOptions.None, FileOptions.Asynchronous, FileOptions.SequentialScan, FileOptions.RandomAccess,
            FileOptions.Asynchronous | FileOptions.SequentialScan, FileOptions.Asynchronous | FileOptions.RandomAccess,
        ];
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            int index = 0;
            foreach (FileOptions fileOptions in supported)
            {
                string path = "/opt" + index++;
                byte[] data = TestData.Bytes(0xF020 + (ulong)index, 4500);
                await using (DfsStream w = await files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None, Options = fileOptions }, Ct))
                    await w.WriteAsync(data, Ct);
                await using DfsStream r = await files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.Read, Options = fileOptions }, Ct);
                await OpenHelpers.AssertBytesAsync(data, OpenHelpers.ReadToEndAsync(r), $"FileOptions {fileOptions}");
            }
        }, Ct);
    }

    /// <summary>
    /// Undefined FileMode values × every access × every share value × missing/existing: explicit argument rejection
    /// (never a default mode), before any request, without creating or changing anything.
    /// </summary>
    [Theory, Catalog("OPEN-019", "U", "INV-07")]
    [MemberData(nameof(InvalidModeValues))]
    public async Task OPEN_019_UnsupportedOsFeaturesFailExplicitly_UndefinedModeValues(int modeValue)
    {
        FileMode mode = (FileMode)modeValue;
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, "/open019m");
            IReadOnlyList<FileAccess> accesses = [.. OpenOracle.ValidAccesses, .. OpenOracle.InvalidAccesses];
            foreach (FileAccess access in accesses)
            {
                foreach (FileShare share in OpenOracle.AllShares)
                {
                    foreach (bool exists in MissingThenExisting)
                        await matrix.RunAsync(mode, access, share, exists, held: [], initialLength: 100);
                }
            }

            matrix.AssertAllPassed(accesses.Count * OpenOracle.AllShares.Count * 2, OpenOutcome.Rejected);
        }, Ct);
    }

    /// <summary>
    /// Invalid DfsOpenOptions values (nonpositive buffer, undefined WriteMode/ReadView, VersionPinned on a writable
    /// handle) are rejected with argument exceptions before any request; nothing is created or changed.
    /// </summary>
    [Fact, Catalog("OPEN-019", "U", "INV-07")]
    public async Task OPEN_019_UnsupportedOsFeaturesFailExplicitly_InvalidOpenOptions()
    {
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] content = TestData.Bytes(0xF021, 3000);
            await files.WriteAllBytesAsync("/existing", content, Ct);
            DfsFileInfo before = await files.GetInfoAsync("/existing", Ct);
            long requestsBefore = OpenHelpers.CountOpenRequests(cluster, "c0");

            DfsOpenOptions[] invalid =
            [
                new() { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.None, BufferSize = 0 },
                new() { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.None, BufferSize = -1 },
                new() { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.None, WriteMode = (DfsWriteMode)7 },
                new() { Mode = FileMode.OpenOrCreate, Access = FileAccess.Read, Share = FileShare.Read, ReadView = (DfsReadView)9 },
                new() { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Share = FileShare.None, ReadView = DfsReadView.VersionPinned },
                new() { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.Read, ReadView = DfsReadView.VersionPinned },
                new() { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None, ReadView = DfsReadView.VersionPinned },
            ];
            foreach (DfsOpenOptions options in invalid)
            {
                foreach (string path in new[] { "/missing", "/existing" })
                {
                    await Assert.ThrowsAnyAsync<ArgumentException>(() => files.OpenAsync(path, options, Ct));
                    Assert.ThrowsAny<ArgumentException>(() => files.Open(path, options));
                }
            }

            long requestsAfter = OpenHelpers.CountOpenRequests(cluster, "c0");
            Assert.False(await files.ExistsAsync("/missing", Ct));
            Assert.Equal(before.Revision, (await files.GetInfoAsync("/existing", Ct)).Revision);
            await OpenHelpers.AssertBytesAsync(content, files.ReadAllBytesAsync("/existing", Ct));
            Assert.Equal(requestsBefore, requestsAfter); // rejected before any request
        }, Ct);
    }

    /// <summary>
    /// No native/OS handle is exposed: DfsStream is not a FileStream and has no public member yielding a SafeHandle or
    /// pointer; timeouts are an unsupported deferred feature (CanTimeout false; Read/WriteTimeout throw).
    /// </summary>
    [Fact, Catalog("OPEN-019", "U", "INV-07", "INV-12")]
    public async Task OPEN_019_UnsupportedOsFeaturesFailExplicitly_NoNativeHandlesOrTimeouts()
    {
        Assert.False(typeof(FileStream).IsAssignableFrom(typeof(DfsStream)), "DfsStream SHALL derive from Stream, not FileStream (FSD §6)");
        static bool IsNative(Type t) => typeof(SafeHandle).IsAssignableFrom(t) || t == typeof(IntPtr) || t == typeof(UIntPtr);
        MemberInfo[] native =
        [
            .. typeof(DfsStream).GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).Where(p => IsNative(p.PropertyType)),
            .. typeof(DfsStream).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).Where(m => IsNative(m.ReturnType)),
        ];
        Assert.Empty(native);

        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            await using DfsStream s = await files.OpenAsync("/t", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, Ct);
            Assert.False(s.CanTimeout);
            Assert.Throws<InvalidOperationException>(() => s.ReadTimeout);
            Assert.Throws<InvalidOperationException>(() => s.ReadTimeout = 1000);
            Assert.Throws<InvalidOperationException>(() => s.WriteTimeout);
            Assert.Throws<InvalidOperationException>(() => s.WriteTimeout = 1000);
            Assert.False(s.IsFaulted);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // Shared drivers
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Opens <paramref name="mode"/>/<paramref name="access"/> with every legal share against every held-handle set on an
    /// existing file (one cluster) and checks the oracle, including "no truncation/creation when rejected".
    /// </summary>
    private static async Task RunActiveHandleMatrixAsync(string directory, FileMode mode, FileAccess access, string accessName, params OpenOutcome[] requiredOutcomes)
    {
        OpenHelpers.Log($"{mode} with access {accessName} against active handles");
        await ClusterTest.RunAsync(OpenHelpers.UnitOptions, async cluster =>
        {
            await using OpenMatrix matrix = await OpenMatrix.StartAsync(cluster, directory);
            foreach (IReadOnlyList<HeldHandle> held in OpenOracle.HeldHandleSets)
            {
                foreach (FileShare share in OpenOracle.ValidShares)
                    await matrix.RunAsync(mode, access, share, exists: true, held);
            }

            matrix.AssertAllPassed(OpenOracle.HeldHandleSets.Count * OpenOracle.ValidShares.Count, requiredOutcomes);
        }, Ct);
    }

    private static Task<DfsStream> OpenViaAsync(DfsFileClient files, string overload, string path, FileMode mode, FileAccess access, FileShare share)
    {
        var options = new DfsOpenOptions { Mode = mode, Access = access, Share = share };
        return overload switch
        {
            "OpenAsync(mode,access,share)" => files.OpenAsync(path, mode, access, share, Ct),
            "OpenAsync(options)" => files.OpenAsync(path, options, Ct),
            "Open(mode,access,share)" => Task.FromResult(files.Open(path, mode, access, share)),
            "Open(options)" => Task.FromResult(files.Open(path, options)),
            _ => throw new ArgumentOutOfRangeException(nameof(overload)),
        };
    }

    private static Task Sync(Action action)
    {
        action();
        return Task.CompletedTask;
    }

    private static Task Sync<T>(Func<T> func)
    {
        _ = func();
        return Task.CompletedTask;
    }
}
