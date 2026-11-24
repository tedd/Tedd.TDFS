using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Simulation.Acl;
using static Tedd.Tdfs.Tests.Simulation.Pool.PoolHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Pool;

public sealed class PoolRestoreWorkloadTests
{
    [Theory]
    [InlineData(false, 12101UL)]
    [InlineData(false, 12109UL)]
    [InlineData(true, 12101UL)]
    [InlineData(true, 12109UL)]
    public Task CommittedSnapshotRestoresFileStreamsAndPermissionsTogether(bool fromBackup, ulong seed)
    {
        IReadOnlyList<TestNodeSpec> nodes = TwoPools();
        TestClusterOptions options = Deterministic(nodes, 3, Volume(VolA, PoolA)) with
        {
            Seed = seed,
            BackupNodeCount = fromBackup ? 1 : 0,
        };
        return ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "publisher" }, Ct);
            DfsVolume source = client.GetVolume(VolA);
            if (fromBackup)
                await cluster.BackupNodes[0].RegisterAsync(VolA);

            const int count = 15;
            var random = new SeededRandom(seed);
            int[] sizes = [0, 1, 4095, 4096, 4097, Chunk, Chunk + 1];
            var manifest = new List<(string Path, byte[] Body, byte[] Stream, DfsSecurityDescriptor Acl)>();
            DfsIdentity owner = DfsIdentity.User("admin");
            await source.Directories.CreateDirectoryAsync("/records", Ct);
            await using DfsTransaction tx = await source.Transactions.BeginAsync(ct: Ct);
            for (int i = 0; i < count; i++)
            {
                string path = $"/records/f{i:D2}";
                byte[] body = TestData.Bytes(seed + (ulong)i, sizes[i % sizes.Length]);
                byte[] metadata = TestData.Bytes(seed + (ulong)(count + i), sizes[random.Next(sizes.Length)]);
                var rights = i % 2 == 0 ? DfsFileSystemRights.Read : DfsFileSystemRights.ReadAttributes;
                var acl = new DfsSecurityDescriptor(owner, [
                    new DfsAccessRule(owner, DfsFileSystemRights.FullControl),
                    new DfsAccessRule(DfsIdentity.Everyone, rights),
                ], AreAccessRulesProtected: true);
                manifest.Add((path, body, metadata, acl));
                await tx.Files.WriteAllBytesAsync(path, body, Ct);
                await tx.Files.WriteAllBytesAsync(path + ":meta", metadata, Ct);
                await tx.Files.SetAccessControlAsync(path, acl, Ct);
            }
            DfsSnapshotInfo snapshot = await tx.CommitWithSnapshotAsync("published", Ct);
            await source.Directories.DeleteAsync("/records", recursive: true, ct: Ct);
            const string destination = "restored";
            if (fromBackup)
            {
                TestBackupNode backup = cluster.BackupNodes[0];
                await source.Backups.RequestSyncAsync(backup.TargetName, snapshot.Name, Ct);
                DfsBackupCheckpoint checkpoint = await source.Backups.AwaitBackupSnapshotAsync(backup.TargetName, snapshot.Name, Converge, Ct);
                Assert.Equal(snapshot.Boundary, checkpoint.CoveredCommit);
                await backup.CrashAsync();
                await backup.RestartAsync();
                await cluster.Admin.RestoreFromBackupAsync(new DfsRestoreFromBackupRequest(
                    [backup.Endpoint], backup.ReaderCredentials, VolA, snapshot.Name, destination), Ct);
            }
            else
            {
                DfsVolumeInfo result = await cluster.RestoreSnapshotToPoolAsync(VolA, snapshot.Name, destination, PoolB, Ct);
                Assert.Equal(PoolB, result.StoragePool);
            }
            DfsVolume restored = client.GetVolume(destination);
            TestPrincipal principal = await cluster.CreatePrincipalAsync("auditor", new Dictionary<string, DfsPermissions>
            {
                [destination] = DfsPermissions.Read,
            });
            await using DfsClient auditor = await cluster.ConnectAsync(new TestClientOptions { Name = "auditor", Credentials = principal.Credentials }, Ct);
            DfsFileClient auditFiles = auditor.GetVolume(destination).Files;
            var paths = new List<string>();
            await foreach (DfsEntryInfo entry in restored.Directories.EnumerateAsync("/records", new DfsEnumerationOptions { PageSize = 2 }, Ct))
                paths.Add(entry.Path);
            Assert.Equal(manifest.Select(item => item.Path), paths);
            foreach (var item in manifest)
            {
                byte[] body = await restored.Files.ReadAllBytesAsync(item.Path, Ct);
                byte[] metadata = await restored.Files.ReadAllBytesAsync(item.Path + ":meta", Ct);
                AssertEx.BytesEqual(item.Body, body, item.Path);
                AssertEx.BytesEqual(item.Stream, metadata, item.Path + ":meta");
                Assert.Equal(item.Acl, await restored.Files.GetAccessControlAsync(item.Path, Ct));
                DfsStreamInfo stream = Assert.Single(await restored.Files.ListStreamsAsync(item.Path, Ct));
                Assert.Equal(("meta", item.Stream.LongLength), (stream.Name, stream.Length));
                if (item.Acl.Rules[1].Rights == DfsFileSystemRights.Read)
                {
                    byte[] allowed = await auditFiles.ReadAllBytesAsync(item.Path + ":meta", Ct);
                    AssertEx.BytesEqual(item.Stream, allowed, item.Path + ":meta as auditor");
                }
                else
                {
                    await AclHelpers.AssertDeniedAsync(() => auditFiles.ReadAllBytesAsync(item.Path + ":meta", Ct), item.Path + ":meta as auditor");
                }
            }
            Assert.False(await source.Directories.ExistsAsync("/records", Ct));
        }, Ct);
    }
}
