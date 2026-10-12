using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Tx;

/// <summary>Shared setup and assertions for the TX suite's unit-level (SDK contract) tests.</summary>
internal static class TxHelpers
{
    /// <summary>Volume created by the default harness configuration.</summary>
    public const string Vol = "vol";

    /// <summary>Bounded wait used for liveness conditions (NodeTimings.Fast: lease 2 s, session 10 s).</summary>
    public static readonly TimeSpan Liveness = TimeSpan.FromSeconds(10);

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Default U-level cluster: three nodes, one per zone, in-memory transport, simulated storage.</summary>
    public static TestClusterOptions Cluster => TestClusterOptions.ThreeZones;

    /// <summary>UTF-8 bytes of <paramref name="text"/>.</summary>
    public static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>Connects a client with network identity <c>client:{name}</c>.</summary>
    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name }, Ct);

    /// <summary>Reads a file, returning null only for genuine absence (missing file or missing parent directory).</summary>
    public static async Task<byte[]?> TryReadAsync(DfsFileClient files, string path)
    {
        try
        {
            return await files.ReadAllBytesAsync(path, Ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Asserts the exact bytes of <paramref name="path"/>.</summary>
    public static async Task AssertContentAsync(DfsFileClient files, string path, byte[] expected, string because)
    {
        byte[] actual = await files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, actual, because);
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts its bytes equal <paramref name="expected"/>.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>Asserts that no file exists at <paramref name="path"/> (Exists false and GetInfo throws FileNotFound).</summary>
    public static async Task AssertFileAbsentAsync(DfsFileClient files, string path, string because)
    {
        Assert.False(await files.ExistsAsync(path, Ct), $"{path} must not exist: {because}");
        Exception ex = await Assert.ThrowsAnyAsync<IOException>(() => files.GetInfoAsync(path, Ct));
        Assert.True(ex is FileNotFoundException or DirectoryNotFoundException,
            $"GetInfo({path}) must report absence with FileNotFoundException/DirectoryNotFoundException ({because}); got {ex.GetType().Name}: {ex.Message}");
    }

    /// <summary>
    /// Captures every namespace entry below <paramref name="root"/> as a canonical string: kind, inode id, revision,
    /// length/entry count, access flags and a SHA-256 of file contents. Used to prove "no visible change".
    /// </summary>
    public static async Task<SortedDictionary<string, string>> CaptureTreeAsync(DfsVolume volume, string root = "/")
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        DfsDirectoryInfo rootInfo = await volume.Directories.GetInfoAsync(root, Ct);
        result[root] = $"dir|inode={rootInfo.InodeId.Value}|rev={rootInfo.Revision}|entries={rootInfo.EntryCount}";
        await CaptureDirectoryAsync(volume, root, result);
        return result;
    }

    private static async Task CaptureDirectoryAsync(DfsVolume volume, string directory, SortedDictionary<string, string> result)
    {
        var children = new List<DfsEntryInfo>();
        await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync(directory, null, Ct))
            children.Add(entry);

        foreach (DfsEntryInfo entry in children)
        {
            if (entry.Kind == DfsEntryKind.Directory)
            {
                DfsDirectoryInfo info = await volume.Directories.GetInfoAsync(entry.Path, Ct);
                result[entry.Path] = $"dir|inode={info.InodeId.Value}|rev={info.Revision}|entries={info.EntryCount}";
                await CaptureDirectoryAsync(volume, entry.Path, result);
            }
            else
            {
                DfsFileInfo info = await volume.Files.GetInfoAsync(entry.Path, Ct);
                byte[] bytes = await volume.Files.ReadAllBytesAsync(entry.Path, Ct);
                result[entry.Path] =
                    $"file|inode={info.InodeId.Value}|rev={info.Revision}|len={info.Length}|access={info.Access}|policy={info.StoragePolicy}|sha={Convert.ToHexString(SHA256.HashData(bytes))}";
            }
        }
    }

    /// <summary>Asserts two captured trees are identical, listing every differing path.</summary>
    public static void AssertTreesEqual(IReadOnlyDictionary<string, string> expected, IReadOnlyDictionary<string, string> actual, string because)
    {
        var differences = new List<string>();
        foreach ((string path, string value) in expected)
        {
            if (!actual.TryGetValue(path, out string? other))
                differences.Add($"missing {path} (expected {value})");
            else if (!string.Equals(value, other, StringComparison.Ordinal))
                differences.Add($"changed {path}: expected {value}, actual {other}");
        }

        foreach ((string path, string value) in actual)
        {
            if (!expected.ContainsKey(path))
                differences.Add($"unexpected {path} ({value})");
        }

        Assert.True(differences.Count == 0, $"Namespace differs ({because}):{Environment.NewLine}{string.Join(Environment.NewLine, differences)}");
    }

    /// <summary>Waits until the volume leader reports no active transactions (the transaction's server state was released).</summary>
    public static Task WaitNoActiveTransactionsAsync(TestCluster cluster, string because, string volume = Vol) =>
        cluster.WaitUntilAsync(async () =>
        {
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
            return state.ActiveTransactions == 0;
        }, Liveness, because, Ct);

    /// <summary>
    /// Asserts that a fresh client can take an exclusive writer handle on <paramref name="path"/>, i.e. no transaction
    /// writer reservation is still held (LOCK-01). The handle is aborted without changing the file.
    /// </summary>
    public static async Task AssertWriterReservationFreeAsync(DfsVolume volume, string path, string because)
    {
        DfsStream stream;
        try
        {
            stream = await volume.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
        }
        catch (DfsSharingViolationException ex)
        {
            Assert.Fail($"Writer reservation on {path} is still held ({because}): {ex.ErrorCode} {ex.Message}");
            throw;
        }

        await using (stream)
        {
            Assert.True(stream.WriterGeneration > 0, $"An exclusive writable handle on {path} must carry a writer generation.");
            await stream.AbortAsync(Ct);
        }
    }

    /// <summary>
    /// True when <paramref name="ex"/> is an acceptable rejection of I/O through a bound handle or facade whose
    /// transaction has completed: <see cref="InvalidOperationException"/> (including <see cref="ObjectDisposedException"/>)
    /// or a <see cref="DfsException"/> reporting an invalid handle or an ended transaction.
    /// </summary>
    public static bool IsCompletedTransactionRejection(Exception ex) =>
        ex is InvalidOperationException
        || ex is DfsException { ErrorCode: DfsErrorCode.InvalidHandle or DfsErrorCode.TransactionAborted };
}
