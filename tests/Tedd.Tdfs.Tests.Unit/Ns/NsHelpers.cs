using System.Globalization;
using System.Security.Cryptography;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Ns;

/// <summary>Shared helpers of the NS suite (unit level).</summary>
internal static class NsHelpers
{
    /// <summary>Name of the default test volume.</summary>
    public const string Vol = "vol";

    /// <summary>Logical update unit (chunk) of <c>NodeTimings.Fast</c>.</summary>
    public const int Chunk = 64 << 10;

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Client options with a distinct network identity.</summary>
    public static TestClientOptions Named(string name, DfsCredentials? credentials = null) =>
        new() { Name = name, Credentials = credentials };

    /// <summary>A grant map for one volume.</summary>
    public static IReadOnlyDictionary<string, DfsPermissions> Grant(DfsPermissions permissions, string volume = Vol) =>
        new Dictionary<string, DfsPermissions>(StringComparer.Ordinal) { [volume] = permissions };

    /// <summary>Byte-for-byte equality on arrays (safe to call with awaited arguments).</summary>
    public static void AssertBytes(byte[] expected, byte[] actual, string? because = null) =>
        AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Collects an async enumeration.</summary>
    public static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
            list.Add(item);
        return list;
    }

    /// <summary>
    /// Independent description of a subtree: path → "kind|inode|revision|length|sha256" for every entry below
    /// <paramref name="root"/> (inclusive). Used to prove that a rejected mutation changed nothing.
    /// </summary>
    public static async Task<SortedDictionary<string, string>> SnapshotTreeAsync(DfsVolume volume, string root)
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        DfsDirectoryInfo rootInfo = await volume.Directories.GetInfoAsync(root, Ct);
        result[root] = string.Create(CultureInfo.InvariantCulture, $"dir|{rootInfo.InodeId.Value}|{rootInfo.Revision}|{rootInfo.EntryCount}");
        await WalkAsync(root);
        return result;

        async Task WalkAsync(string dir)
        {
            foreach (DfsEntryInfo entry in await ToListAsync(volume.Directories.EnumerateAsync(dir, null, Ct)))
            {
                if (entry.Kind == DfsEntryKind.Directory)
                {
                    DfsDirectoryInfo info = await volume.Directories.GetInfoAsync(entry.Path, Ct);
                    result[entry.Path] = string.Create(CultureInfo.InvariantCulture, $"dir|{info.InodeId.Value}|{info.Revision}|{info.EntryCount}");
                    await WalkAsync(entry.Path);
                }
                else
                {
                    DfsFileInfo info = await volume.Files.GetInfoAsync(entry.Path, Ct);
                    byte[] bytes = await volume.Files.ReadAllBytesAsync(entry.Path, Ct);
                    result[entry.Path] = string.Create(CultureInfo.InvariantCulture,
                        $"file|{info.InodeId.Value}|{info.Revision}|{info.Length}|{Convert.ToHexString(SHA256.HashData(bytes))}");
                }
            }
        }
    }

    /// <summary>Asserts two subtree descriptions are identical, naming the first difference.</summary>
    public static void AssertTreeUnchanged(SortedDictionary<string, string> before, SortedDictionary<string, string> after, string because)
    {
        foreach ((string path, string description) in before)
        {
            Assert.True(after.TryGetValue(path, out string? now), $"{because}: '{path}' disappeared");
            Assert.True(string.Equals(description, now, StringComparison.Ordinal), $"{because}: '{path}' changed from {description} to {now}");
        }

        foreach (string path in after.Keys)
            Assert.True(before.ContainsKey(path), $"{because}: unexpected new entry '{path}'");
    }

    /// <summary>Number of inodes in the leader's applied volume state.</summary>
    public static async Task<int> InodeCountAsync(TestCluster cluster, string volume = Vol)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
        return state.InodeCount;
    }

    /// <summary>Creates a small populated tree: dirs and files below <paramref name="root"/>.</summary>
    public static async Task BuildTreeAsync(DfsVolume volume, string root, int directories, int filesPerDirectory, ulong seed, int fileLength = 300)
    {
        await volume.Directories.CreateDirectoryAsync(root, Ct);
        for (int d = 0; d < directories; d++)
        {
            string dir = $"{root}/d{d}";
            await volume.Directories.CreateDirectoryAsync(dir, Ct);
            for (int f = 0; f < filesPerDirectory; f++)
                await volume.Files.WriteAllBytesAsync($"{dir}/f{f}.bin", TestData.Bytes(seed + (ulong)(d * 1000 + f), fileLength), Ct);
        }
    }
}
