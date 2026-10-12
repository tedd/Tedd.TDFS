using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Sec;

/// <summary>Shared helpers for SEC deterministic-simulation scenarios.</summary>
internal static class SecHelpers
{
    public const string Vol = "vol";
    public const string Vol2 = "vol2";

    /// <summary>Deterministic three-zone cluster with two volumes.</summary>
    public static TestClusterOptions Options(int nodeCount = 3) => new()
    {
        NodeCount = nodeCount,
        Mode = ClusterMode.Deterministic,
        Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = VolumeContentFixture.NamedPolicies }, new TestVolumeSpec(Vol2)],
        ScenarioTimeout = TimeSpan.FromMinutes(10),
    };

    /// <summary>Asserts an authenticated principal was refused for lack of permission (AccessDenied).</summary>
    public static void AssertAccessDenied(Exception? e, string because)
    {
        if (e is not UnauthorizedAccessException unauthorized)
        {
            Assert.Fail($"expected UnauthorizedAccessException (AccessDenied) but got {(e is null ? "success" : e.GetType().Name + ": " + e.Message)} ({because})");
            return;
        }

        Assert.True(DfsErrors.ToCode(unauthorized) == DfsErrorCode.AccessDenied, $"expected AccessDenied but got {DfsErrors.ToCode(unauthorized)} ({because})");
    }

    /// <summary>Digest of a subtree (directories and files with revision, length, content hash) as seen by <paramref name="admin"/>.</summary>
    public static async Task<string> SubtreeDigestAsync(DfsVolume admin, string root, CancellationToken ct)
    {
        if (!await admin.Directories.ExistsAsync(root, ct))
            return "<absent>";
        var sb = new StringBuilder();
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            string dir = pending.Pop();
            DfsDirectoryInfo info = await admin.Directories.GetInfoAsync(dir, ct);
            sb.Append("D|").Append(dir).Append('|').Append(info.Revision).Append('\n');
            var children = new List<DfsEntryInfo>();
            await foreach (DfsEntryInfo e in admin.Directories.EnumerateAsync(dir, null, ct))
                children.Add(e);
            foreach (DfsEntryInfo e in children.OrderBy(x => x.Name, StringComparer.Ordinal))
            {
                if (e.Kind == DfsEntryKind.Directory)
                {
                    pending.Push(e.Path);
                    continue;
                }

                DfsFileInfo f = await admin.Files.GetInfoAsync(e.Path, ct);
                byte[] content = await admin.Files.ReadAllBytesAsync(e.Path, ct);
                sb.Append("F|").Append(e.Path).Append('|').Append(f.Revision).Append('|').Append(f.Length).Append('|')
                    .Append(Convert.ToHexString(SHA256.HashData(content))).Append('\n');
            }
        }

        return sb.ToString();
    }

    /// <summary>Names of the volume's snapshots in ordinal order.</summary>
    public static async Task<string[]> SnapshotNamesAsync(DfsVolume volume, CancellationToken ct) =>
        (await volume.Snapshots.ListAsync(ct)).Select(s => s.Name).Order(StringComparer.Ordinal).ToArray();

    /// <summary>Asserts at most one unexpired writer handle exists on the file's inode (INV-05 under clock changes).</summary>
    public static async Task AssertAtMostOneWriterAsync(TestCluster cluster, string volume, string path, string when)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.NotNull(file);
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
        int writers = state.Handles.Count(h => h.Inode == file.Inode && h.IsWriter && !h.Expired);
        Assert.True(writers <= 1, $"{writers} valid writer handles on {path} {when}");
    }
}
