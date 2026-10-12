using System.Text;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.BAK;

/// <summary>
/// BAK — read-only SDK contract of late-sync backup hosts (FSD §12.1; test plan §7.21 BAK-015). Every mutation path of
/// the SDK, and the server-side protocol paths behind them, is attempted against a backup host with each of the
/// backup's own principals; each must be refused and leave the published history byte-identical.
/// </summary>
public sealed class BAKUnitTests
{
    private const string Volume = "vol";
    private const string Snapshot = "s1";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum BackupMutation
    {
        FilesWriteAllBytes,
        FilesWriteAllBytesSync,
        FilesWriteAllText,
        FilesWriteAllLines,
        FilesAppendAllBytes,
        FilesAppendAllText,
        FilesAppendAllLines,
        FilesCopy,
        FilesCopyOverwrite,
        FilesMove,
        FilesMoveSync,
        FilesDelete,
        FilesDeleteSync,
        FilesSetStoragePolicy,
        FilesSetAccess,
        FilesCreate,
        FilesOpenWrite,
        OpenCreateNewWrite,
        OpenCreateReadWrite,
        OpenOrCreateWrite,
        OpenTruncateWrite,
        OpenAppendWrite,
        OpenExistingWrite,
        OpenExistingReadWrite,
        OpenWriteThroughOptions,
        OpenSync,
        DirectoriesCreate,
        DirectoriesMove,
        DirectoriesDelete,
        DirectoriesDeleteRecursive,
        TransactionBegin,
        TransactionBeginSync,
        SnapshotCreate,
        SnapshotCreateSync,
        SnapshotDelete,
        SnapshotRestore,
        ViewWriteAllBytes,
        ViewAppendAllBytes,
        ViewDelete,
        ViewMove,
        ViewOpenReadWrite,
        ViewOpenTruncate,
        ViewCreateDirectory,
        ViewDeleteDirectory,
        BackupsRegisterTarget,
        BackupsRequestSync,
        AdminSetDefaultPolicy,
        AdminDefinePolicy,
        AdminCreateVolume,
        AdminDeleteVolume,
    }

    public static TheoryData<BackupMutation> AllMutations
    {
        get
        {
            var data = new TheoryData<BackupMutation>();
            foreach (BackupMutation mutation in Enum.GetValues<BackupMutation>())
                data.Add(mutation);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(AllMutations))]
    [Catalog("BAK-015", "U", "INV-15", "INV-12", "INV-13")]
    public Task BAK_015_AllMutationsOnBackupDenied(BackupMutation mutation) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }, async cluster =>
        {
            (TestBackupNode b0, SortedDictionary<string, byte[]> files) = await PrepareAsync(cluster);
            foreach ((string principal, DfsCredentials credentials) in Principals(b0))
            {
                string because = $"{mutation} through the backup host as {principal}";
                await using DfsClient backup = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk-" + principal, Credentials = credentials }, Ct);
                DfsVolume bvol = backup.GetVolume(Volume);
                Assert.True(bvol.Files.IsReadOnly, $"{because}: Files.IsReadOnly");
                Assert.True(bvol.Directories.IsReadOnly, $"{because}: Directories.IsReadOnly");
                Assert.True(bvol.Snapshots.IsReadOnly, $"{because}: Snapshots.IsReadOnly");
                Exception refusal;
                await using (DfsSnapshotView view = await bvol.Snapshots.OpenAsync(Snapshot, Ct))
                {
                    Assert.True(view.Files.IsReadOnly && view.Directories.IsReadOnly, $"{because}: snapshot view facades are read-only");
                    refusal = await ExpectRefusalAsync(() => PerformAsync(mutation, backup, bvol, view), because);
                }

                AssertRefusalKind(mutation, refusal, because);
                await AssertUnchangedAsync(b0, backup, files, because);
            }

            // The live source is unaffected by anything attempted on the backup.
            await using DfsClient live = await cluster.ConnectAsync(new TestClientOptions { Name = "c-verify" }, Ct);
            foreach (KeyValuePair<string, byte[]> file in files)
            {
                byte[] actual = await live.GetVolume(Volume).Files.ReadAllBytesAsync(file.Key, Ct);
                AssertEx.BytesEqual(file.Value, actual, $"live {file.Key} after {mutation} on the backup");
            }
        });

    [Theory]
    [InlineData(DfsOperationKind.CreateDirectory)]
    [InlineData(DfsOperationKind.Delete)]
    [InlineData(DfsOperationKind.Open)]
    [InlineData(DfsOperationKind.Move)]
    [InlineData(DfsOperationKind.WriteAll)]
    [InlineData(DfsOperationKind.GetInfo)]
    [Catalog("BAK-015", "U", "INV-15", "INV-12")]
    public Task BAK_015_AllMutationsOnBackupDenied_ServerSideEnforcement(DfsOperationKind kind) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }, async cluster =>
        {
            (TestBackupNode b0, SortedDictionary<string, byte[]> files) = await PrepareAsync(cluster);
            (string path, string? path2) = kind switch
            {
                DfsOperationKind.CreateDirectory => ("/raw-dir", (string?)null),
                DfsOperationKind.Delete => ("/docs/a.bin", null),
                DfsOperationKind.Open => ("/raw-new.bin", null),
                DfsOperationKind.Move => ("/docs/a.bin", "/raw-moved.bin"),
                DfsOperationKind.WriteAll => ("/docs/a.bin", null),
                _ => ("/docs/a.bin", null),
            };

            foreach ((string principal, DfsCredentials credentials) in Principals(b0))
            {
                string because = $"raw {kind} to the backup host as {principal}";
                await using (RawProtocolClient raw = await cluster.ConnectRawBackupAsync(0, Ct))
                {
                    Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(credentials));
                    DfsErrorCode code = await raw.SendPathOperationAsync(Volume, kind, path, path2);
                    if (kind == DfsOperationKind.GetInfo)
                    {
                        // Control: a non-mutating request is not refused as read-only; the backup has no live namespace
                        // to answer from, so it may only be served (None) or reported as unsupported/absent.
                        Assert.True(code is DfsErrorCode.None or DfsErrorCode.Unsupported or DfsErrorCode.FileNotFound or DfsErrorCode.VolumeNotFound,
                            $"{because}: unexpected {code}");
                    }
                    else
                    {
                        Assert.True(code == DfsErrorCode.ReadOnly, $"{because}: the backup server must refuse the mutation itself, got {code}");
                    }
                }

                await using DfsClient backup = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk-" + principal, Credentials = credentials }, Ct);
                await AssertUnchangedAsync(b0, backup, files, because);
            }
        });

    private static IEnumerable<(string Principal, DfsCredentials Credentials)> Principals(TestBackupNode backup) =>
    [
        ("reader", backup.ReaderCredentials),
        ("export", backup.ExportCredentials),
        ("retention-admin", backup.RetentionAdminCredentials),
    ];

    /// <summary>Writes a small tree on the live volume, snapshots it and waits until the backup published it exactly.</summary>
    private static async Task<(TestBackupNode Backup, SortedDictionary<string, byte[]> Files)> PrepareAsync(TestCluster cluster)
    {
        TestBackupNode b0 = cluster.BackupNodes[0];
        await using DfsClient live = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
        DfsVolume vol = live.GetVolume(Volume);
        await b0.RegisterAsync(Volume, new DfsBackupSchedule(Interval: null, OnEverySnapshot: false), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));
        var files = new SortedDictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["/docs/a.bin"] = TestData.Bytes(1, (150 * 1024) + 3),
            ["/docs/b.txt"] = Encoding.UTF8.GetBytes("line one\nline two\n"),
            ["/small.bin"] = TestData.Bytes(2, 300),
        };
        await vol.Directories.CreateDirectoryAsync("/docs", Ct);
        await vol.Directories.CreateDirectoryAsync("/emptydir", Ct);
        foreach (KeyValuePair<string, byte[]> file in files)
            Assert.False((await vol.Files.WriteAllBytesAsync(file.Key, file.Value, Ct)).IsNone);
        DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync(Snapshot, null, Ct);
        await vol.Backups.RequestSyncAsync(b0.TargetName, Snapshot, Ct);
        DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, Snapshot, TimeSpan.FromSeconds(60), Ct);
        Assert.Equal(s1.Id, checkpoint.SnapshotId);
        Assert.Equal(s1.Boundary, checkpoint.CoveredCommit);
        Assert.True(checkpoint.IsExactSnapshot);
        return (b0, files);
    }

    private static async Task<Exception> ExpectRefusalAsync(Func<Task> action, string because)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            return ex;
        }

        Assert.Fail($"{because}: the mutation succeeded on a read-only backup host");
        throw new InvalidOperationException("unreachable");
    }

    /// <summary>Namespace, data, transaction and snapshot mutations are ReadOnly; configuration paths may also be authorization refusals.</summary>
    private static void AssertRefusalKind(BackupMutation mutation, Exception refusal, string because)
    {
        bool configurationPath = mutation is BackupMutation.SnapshotDelete or BackupMutation.BackupsRegisterTarget or BackupMutation.BackupsRequestSync
            or BackupMutation.AdminSetDefaultPolicy or BackupMutation.AdminDefinePolicy or BackupMutation.AdminCreateVolume or BackupMutation.AdminDeleteVolume;
        if (refusal is DfsReadOnlyException readOnly)
        {
            Assert.Equal(DfsErrorCode.ReadOnly, readOnly.ErrorCode);
            return;
        }

        Assert.True(configurationPath && refusal is UnauthorizedAccessException,
            $"{because}: expected DfsReadOnlyException (ReadOnly){(configurationPath ? " or UnauthorizedAccessException" : string.Empty)}, " +
            $"got {refusal.GetType().FullName} ({DfsErrors.ToCode(refusal)}): {refusal.Message}");
    }

    private static async Task PerformAsync(BackupMutation mutation, DfsClient backup, DfsVolume bvol, DfsSnapshotView view)
    {
        byte[] data = TestData.Bytes(99, 5000);
        DfsFileClient files = bvol.Files;
        switch (mutation)
        {
            case BackupMutation.FilesWriteAllBytes:
                await files.WriteAllBytesAsync("/docs/a.bin", data, Ct);
                break;
            case BackupMutation.FilesWriteAllBytesSync:
                files.WriteAllBytes("/docs/a.bin", data);
                break;
            case BackupMutation.FilesWriteAllText:
                await files.WriteAllTextAsync("/docs/b.txt", "replaced", null, Ct);
                break;
            case BackupMutation.FilesWriteAllLines:
                await files.WriteAllLinesAsync("/docs/b.txt", ["replaced"], null, Ct);
                break;
            case BackupMutation.FilesAppendAllBytes:
                await files.AppendAllBytesAsync("/docs/a.bin", data, Ct);
                break;
            case BackupMutation.FilesAppendAllText:
                await files.AppendAllTextAsync("/docs/b.txt", "more", null, Ct);
                break;
            case BackupMutation.FilesAppendAllLines:
                await files.AppendAllLinesAsync("/docs/b.txt", ["more"], null, Ct);
                break;
            case BackupMutation.FilesCopy:
                await files.CopyAsync("/docs/a.bin", "/docs/copy.bin", false, Ct);
                break;
            case BackupMutation.FilesCopyOverwrite:
                await files.CopyAsync("/small.bin", "/docs/a.bin", true, Ct);
                break;
            case BackupMutation.FilesMove:
                await files.MoveAsync("/docs/a.bin", "/moved.bin", false, Ct);
                break;
            case BackupMutation.FilesMoveSync:
                files.Move("/docs/a.bin", "/moved.bin");
                break;
            case BackupMutation.FilesDelete:
                await files.DeleteAsync("/docs/a.bin", Ct);
                break;
            case BackupMutation.FilesDeleteSync:
                files.Delete("/docs/a.bin");
                break;
            case BackupMutation.FilesSetStoragePolicy:
                await files.SetStoragePolicyAsync("/docs/a.bin", "default", Ct);
                break;
            case BackupMutation.FilesSetAccess:
                await files.SetAccessAsync("/docs/a.bin", new DfsInodeAccess(OwnerOnly: true), Ct);
                break;
            case BackupMutation.FilesCreate:
                await using (DfsStream s = await files.CreateAsync("/created.bin", Ct))
                    await s.WriteAsync(data, Ct);
                break;
            case BackupMutation.FilesOpenWrite:
                await using (DfsStream s = await files.OpenWriteAsync("/docs/a.bin", Ct))
                    await s.WriteAsync(data, Ct);
                break;
            case BackupMutation.OpenCreateNewWrite:
                await OpenAndWriteAsync(files, "/created.bin", FileMode.CreateNew, FileAccess.Write, data);
                break;
            case BackupMutation.OpenCreateReadWrite:
                await OpenAndWriteAsync(files, "/docs/a.bin", FileMode.Create, FileAccess.ReadWrite, data);
                break;
            case BackupMutation.OpenOrCreateWrite:
                await OpenAndWriteAsync(files, "/docs/a.bin", FileMode.OpenOrCreate, FileAccess.Write, data);
                break;
            case BackupMutation.OpenTruncateWrite:
                await OpenAndWriteAsync(files, "/docs/a.bin", FileMode.Truncate, FileAccess.Write, data);
                break;
            case BackupMutation.OpenAppendWrite:
                await OpenAndWriteAsync(files, "/docs/a.bin", FileMode.Append, FileAccess.Write, data);
                break;
            case BackupMutation.OpenExistingWrite:
                await OpenAndWriteAsync(files, "/docs/a.bin", FileMode.Open, FileAccess.Write, data);
                break;
            case BackupMutation.OpenExistingReadWrite:
                await OpenAndWriteAsync(files, "/docs/a.bin", FileMode.Open, FileAccess.ReadWrite, data);
                break;
            case BackupMutation.OpenWriteThroughOptions:
                await using (DfsStream s = await files.OpenAsync("/docs/a.bin", new DfsOpenOptions
                       {
                           Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough,
                       }, Ct))
                {
                    await s.WriteAsync(data, Ct);
                }

                break;
            case BackupMutation.OpenSync:
                using (DfsStream s = files.Open("/docs/a.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                    s.Write(data);
                break;
            case BackupMutation.DirectoriesCreate:
                await bvol.Directories.CreateDirectoryAsync("/newdir", Ct);
                break;
            case BackupMutation.DirectoriesMove:
                await bvol.Directories.MoveAsync("/docs", "/docs2", Ct);
                break;
            case BackupMutation.DirectoriesDelete:
                await bvol.Directories.DeleteAsync("/emptydir", false, Ct);
                break;
            case BackupMutation.DirectoriesDeleteRecursive:
                await bvol.Directories.DeleteAsync("/docs", true, Ct);
                break;
            case BackupMutation.TransactionBegin:
                await using (DfsTransaction tx = await bvol.Transactions.BeginAsync(null, Ct))
                {
                    await tx.Files.WriteAllBytesAsync("/docs/a.bin", data, Ct);
                    await tx.CommitAsync(Ct);
                }

                break;
            case BackupMutation.TransactionBeginSync:
                using (DfsTransaction tx = bvol.Transactions.Begin())
                    tx.Commit();
                break;
            case BackupMutation.SnapshotCreate:
                await bvol.Snapshots.CreateAsync("s-new", null, Ct);
                break;
            case BackupMutation.SnapshotCreateSync:
                bvol.Snapshots.Create("s-new");
                break;
            case BackupMutation.SnapshotDelete:
                await bvol.Snapshots.DeleteAsync(Snapshot, Ct);
                break;
            case BackupMutation.SnapshotRestore:
                await bvol.Snapshots.RestoreAsync(Snapshot, "restored", Ct);
                break;
            case BackupMutation.ViewWriteAllBytes:
                await view.Files.WriteAllBytesAsync("/docs/a.bin", data, Ct);
                break;
            case BackupMutation.ViewAppendAllBytes:
                await view.Files.AppendAllBytesAsync("/docs/a.bin", data, Ct);
                break;
            case BackupMutation.ViewDelete:
                await view.Files.DeleteAsync("/docs/a.bin", Ct);
                break;
            case BackupMutation.ViewMove:
                await view.Files.MoveAsync("/docs/a.bin", "/moved.bin", false, Ct);
                break;
            case BackupMutation.ViewOpenReadWrite:
                await OpenAndWriteAsync(view.Files, "/docs/a.bin", FileMode.Open, FileAccess.ReadWrite, data);
                break;
            case BackupMutation.ViewOpenTruncate:
                await OpenAndWriteAsync(view.Files, "/docs/a.bin", FileMode.Truncate, FileAccess.Write, data);
                break;
            case BackupMutation.ViewCreateDirectory:
                await view.Directories.CreateDirectoryAsync("/newdir", Ct);
                break;
            case BackupMutation.ViewDeleteDirectory:
                await view.Directories.DeleteAsync("/docs", true, Ct);
                break;
            case BackupMutation.BackupsRegisterTarget:
                await bvol.Backups.RegisterTargetAsync(new DfsBackupTargetRegistration("rogue", ["mem://rogue"], new DfsCredentials("rogue", "secret"),
                    new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 20)), Ct);
                break;
            case BackupMutation.BackupsRequestSync:
                await bvol.Backups.RequestSyncAsync("b0", Snapshot, Ct);
                break;
            case BackupMutation.AdminSetDefaultPolicy:
                await backup.Admin.SetDefaultPolicyAsync(Volume, DfsStoragePolicy.Default with { TargetCopies = 2, TargetDistinctZones = 2 }, false, Ct);
                break;
            case BackupMutation.AdminDefinePolicy:
                await backup.Admin.DefinePolicyAsync(Volume, "rogue", DfsStoragePolicy.Default, Ct);
                break;
            case BackupMutation.AdminCreateVolume:
                await backup.Admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "rogue" }, Ct);
                break;
            case BackupMutation.AdminDeleteVolume:
                await backup.Admin.DeleteVolumeAsync(Volume, Ct);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mutation), mutation, "unhandled mutation kind");
        }
    }

    private static async Task OpenAndWriteAsync(DfsFileClient files, string path, FileMode mode, FileAccess access, byte[] data)
    {
        await using DfsStream stream = await files.OpenAsync(path, mode, access, FileShare.None, Ct);
        await stream.WriteAsync(data, Ct);
        await stream.CommitAsync(Ct);
    }

    /// <summary>The backup's published history, listing and every byte are exactly as published before the attempt.</summary>
    private static async Task AssertUnchangedAsync(TestBackupNode backup, DfsClient client, SortedDictionary<string, byte[]> files, string because)
    {
        Assert.Equal(Snapshot, await backup.Inspect.GetPublishedPointerAsync(Volume));
        Assert.Equal([Snapshot], await backup.Inspect.GetPublishedSnapshotsAsync(Volume));
        DfsSnapshotClient snapshots = client.GetVolume(Volume).Snapshots;
        IReadOnlyList<DfsSnapshotInfo> listed = await snapshots.ListAsync(Ct);
        Assert.True(listed.Count == 1 && listed[0].Name == Snapshot, $"{because}: listed [{string.Join(", ", listed.Select(s => s.Name))}]");

        await using DfsSnapshotView view = await snapshots.OpenAsync(Snapshot, Ct);
        var root = new List<string>();
        await foreach (DfsEntryInfo entry in view.Directories.EnumerateAsync("/", null, Ct))
            root.Add(entry.Name);
        Assert.Equal(["docs", "emptydir", "small.bin"], root.Order(StringComparer.Ordinal));
        var docs = new List<string>();
        await foreach (DfsEntryInfo entry in view.Directories.EnumerateAsync("/docs", null, Ct))
            docs.Add(entry.Name);
        Assert.Equal(["a.bin", "b.txt"], docs.Order(StringComparer.Ordinal));
        Assert.True(await view.Directories.ExistsAsync("/emptydir", Ct), $"{because}: /emptydir");
        foreach (KeyValuePair<string, byte[]> file in files)
        {
            byte[] actual = await view.Files.ReadAllBytesAsync(file.Key, Ct);
            AssertEx.BytesEqual(file.Value, actual, $"{because}: {file.Key}");
        }
    }
}
