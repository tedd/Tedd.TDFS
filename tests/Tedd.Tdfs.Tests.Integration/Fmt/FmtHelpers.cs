using System.Security.Cryptography;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Fmt;

/// <summary>Expected contents of a reference image written by <see cref="FmtHelpers.WriteReferenceImageAsync"/>.</summary>
internal sealed record ReferenceImage(
    IReadOnlyDictionary<string, (byte[] Content, InodeId Inode, ulong Revision)> Live,
    IReadOnlyList<string> Directories,
    IReadOnlyList<string> Absent,
    string SnapshotName,
    CommitToken SnapshotBoundary,
    IReadOnlyDictionary<string, byte[]> SnapshotContent);

/// <summary>Shared helpers for the FMT integration tests (real QUIC transport, real disk storage).</summary>
internal static class FmtHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Three nodes, one per zone, real QUIC transport and on-disk storage.</summary>
    public static TestClusterOptions QuicDisk(int backupNodes = 0) => new()
    {
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        BackupNodeCount = backupNodes,
        EncryptAtRest = true,
        ScenarioTimeout = TimeSpan.FromMinutes(5),
    };

    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Restarts every live node, one at a time, as <paramref name="profile"/>.</summary>
    public static async Task RestartAllAsAsync(TestCluster cluster, SoftwareVersionProfile profile)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            await node.RestartAsSoftwareVersionAsync(profile);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
        }

        Assert.All(cluster.Nodes, n => Assert.Equal(profile.Name, n.SoftwareVersion.Name));
    }

    /// <summary>SHA-256 of every raw durable file on a (stopped) node's disk.</summary>
    public static SortedDictionary<string, string> Fingerprint(TestDisk disk)
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (string file in disk.Files())
            result[file] = Convert.ToHexString(SHA256.HashData(disk.ReadRaw(file)));
        return result;
    }

    /// <summary>
    /// Writes a reference image exercising every on-disk structure: nested directories, an inline file, a multi-chunk file,
    /// a sparse file, an overwritten file, an appended file, a renamed file, a deleted file, and a snapshot followed by
    /// further changes. Returns the exact expected live state (bytes, inode ids, revisions) and snapshot contents.
    /// </summary>
    public static async Task<ReferenceImage> WriteReferenceImageAsync(DfsVolume vol, string root)
    {
        var content = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        string[] directories = [root, $"{root}/dir", $"{root}/dir/sub"];
        await vol.Directories.CreateDirectoryAsync($"{root}/dir/sub", Ct);

        async Task WriteAsync(string path, byte[] bytes)
        {
            await vol.Files.WriteAllBytesAsync(path, bytes, Ct);
            content[path] = bytes;
        }

        await WriteAsync($"{root}/inline", TestData.Bytes(5001, 100));
        await WriteAsync($"{root}/dir/multi", TestData.Bytes(5002, 200 * 1024));
        await WriteAsync($"{root}/over", TestData.Bytes(5003, 8 * 1024));
        await WriteAsync($"{root}/over", TestData.Bytes(5004, 9 * 1024));
        await WriteAsync($"{root}/appended", TestData.Bytes(5005, 5000));
        byte[] suffix = TestData.Bytes(5006, 3000);
        await vol.Files.AppendAllBytesAsync($"{root}/appended", suffix, Ct);
        content[$"{root}/appended"] = [.. content[$"{root}/appended"], .. suffix];
        await WriteAsync($"{root}/before-rename", TestData.Bytes(5007, 6000));
        await vol.Files.MoveAsync($"{root}/before-rename", $"{root}/dir/sub/renamed", overwrite: false, Ct);
        content[$"{root}/dir/sub/renamed"] = content[$"{root}/before-rename"];
        content.Remove($"{root}/before-rename");
        await WriteAsync($"{root}/deleted", TestData.Bytes(5008, 7000));
        await vol.Files.DeleteAsync($"{root}/deleted", Ct);
        content.Remove($"{root}/deleted");

        byte[] sparse = new byte[300 * 1024];
        byte[] island = TestData.Bytes(5009, 1024);
        island.CopyTo(sparse, 250 * 1024);
        await using (DfsStream stream = await vol.Files.OpenAsync($"{root}/sparse", FileMode.Create, FileAccess.ReadWrite, FileShare.Read, Ct))
        {
            await stream.SetLengthAsync(sparse.Length, Ct);
            stream.Position = 250 * 1024;
            await stream.WriteAsync(island, Ct);
            await stream.FlushAsync(Ct);
        }

        content[$"{root}/sparse"] = sparse;

        string snapshotName = $"img-{root.Trim('/').Replace('/', '-')}";
        DfsSnapshotInfo snapshot = await vol.Snapshots.CreateAsync(snapshotName, null, Ct);
        var snapshotContent = new Dictionary<string, byte[]>(content, StringComparer.Ordinal);

        await WriteAsync($"{root}/over", TestData.Bytes(5010, 10 * 1024));
        await vol.Files.DeleteAsync($"{root}/inline", Ct);
        content.Remove($"{root}/inline");
        await WriteAsync($"{root}/after-snapshot", TestData.Bytes(5011, 70 * 1024));

        var live = new Dictionary<string, (byte[], InodeId, ulong)>(StringComparer.Ordinal);
        foreach ((string path, byte[] bytes) in content)
        {
            DfsFileInfo info = await vol.Files.GetInfoAsync(path, Ct);
            live[path] = (bytes, info.InodeId, info.Revision);
        }

        return new ReferenceImage(live, directories, [$"{root}/deleted", $"{root}/before-rename", $"{root}/inline"], snapshotName,
            snapshot.Boundary, snapshotContent);
    }

    /// <summary>Verifies that <paramref name="vol"/> serves exactly the reference image: bytes, identities, revisions, absences and snapshot.</summary>
    public static async Task VerifyReferenceImageAsync(DfsVolume vol, ReferenceImage image, string because)
    {
        foreach ((string path, (byte[] content, InodeId inode, ulong revision)) in image.Live)
        {
            BytesEqual(content, await vol.Files.ReadAllBytesAsync(path, Ct), $"{path}: {because}");
            DfsFileInfo info = await vol.Files.GetInfoAsync(path, Ct);
            Assert.Equal(inode, info.InodeId);
            Assert.Equal(revision, info.Revision);
            Assert.Equal(content.LongLength, info.Length);
        }

        foreach (string directory in image.Directories)
            Assert.True(await vol.Directories.ExistsAsync(directory, Ct), $"{directory} must exist ({because})");
        foreach (string path in image.Absent)
            Assert.False(await vol.Files.ExistsAsync(path, Ct), $"{path} must stay absent ({because})");

        DfsSnapshotInfo snapshot = await vol.Snapshots.GetAsync(image.SnapshotName, Ct);
        Assert.Equal(image.SnapshotBoundary, snapshot.Boundary);
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(image.SnapshotName, Ct);
        foreach ((string path, byte[] content) in image.SnapshotContent)
            BytesEqual(content, await view.Files.ReadAllBytesAsync(path, Ct), $"snapshot {path}: {because}");
    }
}
