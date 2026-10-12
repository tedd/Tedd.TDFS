using System.Globalization;
using System.Text;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Sdk;

namespace Tedd.Tdfs.Tests.Unit.Paths;

/// <summary>Shared helpers for the PATH unit suite: path builders and the "rejected locally, no request" sweep.</summary>
internal static class PathHelpers
{
    /// <summary>Default volume.</summary>
    public const string Volume = SdkHelpers.Volume;

    /// <summary>One path-taking SDK entry point; the bad path is passed in the position named by <see cref="Name"/>.</summary>
    public sealed record PathOperation(string Name, Func<PathOperationContext, string, Task> Invoke);

    /// <summary>Valid companions for two-path operations plus an open transaction for transaction-bound entry points.</summary>
    public sealed class PathOperationContext
    {
        public required DfsVolume Volume { get; init; }
        public required DfsTransaction Transaction { get; init; }
        public string ExistingFile { get; init; } = "/path-valid/existing.bin";
        public string ExistingDirectory { get; init; } = "/path-valid/dir";
        public string FreshTarget { get; init; } = "/path-valid/fresh-target";
        public DfsFileClient Files => Volume.Files;
        public DfsDirectoryClient Directories => Volume.Directories;
    }

    private static readonly byte[] Payload = TestData.Bytes(9001, 64);

    /// <summary>
    /// Every path-taking SDK entry point (async and representative sync counterparts, file, directory and
    /// transaction-bound facades, both positions of two-path operations).
    /// </summary>
    public static IReadOnlyList<PathOperation> Operations { get; } =
    [
        new("Files.ExistsAsync", (c, p) => c.Files.ExistsAsync(p, SdkHelpers.Ct)),
        new("Files.GetInfoAsync", (c, p) => c.Files.GetInfoAsync(p, SdkHelpers.Ct)),
        new("Files.OpenReadAsync", async (c, p) => { await using DfsStream s = await c.Files.OpenReadAsync(p, SdkHelpers.Ct); }),
        new("Files.OpenWriteAsync", async (c, p) => { await using DfsStream s = await c.Files.OpenWriteAsync(p, SdkHelpers.Ct); }),
        new("Files.CreateAsync", async (c, p) => { await using DfsStream s = await c.Files.CreateAsync(p, SdkHelpers.Ct); }),
        new("Files.OpenAsync(CreateNew)", async (c, p) =>
        {
            await using DfsStream s = await c.Files.OpenAsync(p, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None }, SdkHelpers.Ct);
        }),
        new("Files.OpenAsync(mode,access,share)", async (c, p) =>
        {
            await using DfsStream s = await c.Files.OpenAsync(p, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, SdkHelpers.Ct);
        }),
        new("Files.ReadAllBytesAsync", (c, p) => c.Files.ReadAllBytesAsync(p, SdkHelpers.Ct)),
        new("Files.ReadAllTextAsync", (c, p) => c.Files.ReadAllTextAsync(p, null, SdkHelpers.Ct)),
        new("Files.ReadAllLinesAsync", (c, p) => c.Files.ReadAllLinesAsync(p, null, SdkHelpers.Ct)),
        new("Files.ReadLinesAsync", async (c, p) =>
        {
            await foreach (string _ in c.Files.ReadLinesAsync(p, null, SdkHelpers.Ct))
                break;
        }),
        new("Files.WriteAllBytesAsync", (c, p) => c.Files.WriteAllBytesAsync(p, Payload, SdkHelpers.Ct)),
        new("Files.WriteAllBytesAsync(options)", (c, p) => c.Files.WriteAllBytesAsync(p, Payload, new DfsMutationOptions(), SdkHelpers.Ct)),
        new("Files.WriteAllTextAsync", (c, p) => c.Files.WriteAllTextAsync(p, "text", null, SdkHelpers.Ct)),
        new("Files.WriteAllLinesAsync", (c, p) => c.Files.WriteAllLinesAsync(p, ["a", "b"], null, SdkHelpers.Ct)),
        new("Files.AppendAllBytesAsync", (c, p) => c.Files.AppendAllBytesAsync(p, Payload, SdkHelpers.Ct)),
        new("Files.AppendAllTextAsync", (c, p) => c.Files.AppendAllTextAsync(p, "text", null, SdkHelpers.Ct)),
        new("Files.AppendAllLinesAsync", (c, p) => c.Files.AppendAllLinesAsync(p, ["a"], null, SdkHelpers.Ct)),
        new("Files.DeleteAsync", (c, p) => c.Files.DeleteAsync(p, SdkHelpers.Ct)),
        new("Files.CopyAsync(source)", (c, p) => c.Files.CopyAsync(p, c.FreshTarget, false, SdkHelpers.Ct)),
        new("Files.CopyAsync(destination)", (c, p) => c.Files.CopyAsync(c.ExistingFile, p, true, SdkHelpers.Ct)),
        new("Files.MoveAsync(source)", (c, p) => c.Files.MoveAsync(p, c.FreshTarget, false, SdkHelpers.Ct)),
        new("Files.MoveAsync(destination)", (c, p) => c.Files.MoveAsync(c.ExistingFile, p, true, SdkHelpers.Ct)),
        new("Files.SetStoragePolicyAsync", (c, p) => c.Files.SetStoragePolicyAsync(p, "default", SdkHelpers.Ct)),
        new("Files.SetAccessAsync", (c, p) => c.Files.SetAccessAsync(p, new DfsInodeAccess(), SdkHelpers.Ct)),
        new("Files.Exists(sync)", (c, p) => { _ = c.Files.Exists(p); return Task.CompletedTask; }),
        new("Files.WriteAllBytes(sync)", (c, p) => { _ = c.Files.WriteAllBytes(p, Payload); return Task.CompletedTask; }),
        new("Files.Open(sync)", (c, p) =>
        {
            using DfsStream s = c.Files.Open(p, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            return Task.CompletedTask;
        }),
        new("Files.ReadLines(sync)", (c, p) =>
        {
            foreach (string _ in c.Files.ReadLines(p))
                break;
            return Task.CompletedTask;
        }),
        new("Directories.CreateDirectoryAsync", (c, p) => c.Directories.CreateDirectoryAsync(p, SdkHelpers.Ct)),
        new("Directories.ExistsAsync", (c, p) => c.Directories.ExistsAsync(p, SdkHelpers.Ct)),
        new("Directories.GetInfoAsync", (c, p) => c.Directories.GetInfoAsync(p, SdkHelpers.Ct)),
        new("Directories.DeleteAsync(recursive)", (c, p) => c.Directories.DeleteAsync(p, true, SdkHelpers.Ct)),
        new("Directories.MoveAsync(source)", (c, p) => c.Directories.MoveAsync(p, c.FreshTarget, SdkHelpers.Ct)),
        new("Directories.MoveAsync(destination)", (c, p) => c.Directories.MoveAsync(c.ExistingDirectory, p, SdkHelpers.Ct)),
        new("Directories.EnumerateAsync", async (c, p) =>
        {
            await foreach (DfsEntryInfo _ in c.Directories.EnumerateAsync(p, null, SdkHelpers.Ct))
                break;
        }),
        new("Directories.ListPageAsync", (c, p) => c.Directories.ListPageAsync(p, null, null, SdkHelpers.Ct)),
        new("Directories.CreateDirectory(sync)", (c, p) => { _ = c.Directories.CreateDirectory(p); return Task.CompletedTask; }),
        new("Directories.Enumerate(sync)", (c, p) =>
        {
            foreach (DfsEntryInfo _ in c.Directories.Enumerate(p))
                break;
            return Task.CompletedTask;
        }),
        new("Transaction.Files.WriteAllBytesAsync", (c, p) => c.Transaction.Files.WriteAllBytesAsync(p, Payload, SdkHelpers.Ct)),
        new("Transaction.Files.ExistsAsync", (c, p) => c.Transaction.Files.ExistsAsync(p, SdkHelpers.Ct)),
        new("Transaction.Files.OpenAsync", async (c, p) =>
        {
            await using DfsStream s = await c.Transaction.Files.OpenAsync(p, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, SdkHelpers.Ct);
        }),
        new("Transaction.Directories.CreateDirectoryAsync", (c, p) => c.Transaction.Directories.CreateDirectoryAsync(p, SdkHelpers.Ct)),
        new("Transaction.Directories.DeleteAsync", (c, p) => c.Transaction.Directories.DeleteAsync(p, true, SdkHelpers.Ct)),
    ];

    /// <summary>Creates the valid companions (existing file and directory) and opens a transaction.</summary>
    public static async Task<PathOperationContext> PrepareAsync(DfsVolume volume)
    {
        await volume.Directories.CreateDirectoryAsync("/path-valid/dir", SdkHelpers.Ct);
        await volume.Files.WriteAllBytesAsync("/path-valid/existing.bin", TestData.Bytes(9002, 128), SdkHelpers.Ct);
        DfsTransaction tx = await volume.Transactions.BeginAsync(null, SdkHelpers.Ct);
        return new PathOperationContext { Volume = volume, Transaction = tx };
    }

    /// <summary>
    /// True when an <see cref="ArgumentException"/> identifies the path as the invalid argument: it is classified
    /// <see cref="DfsErrorCode.InvalidPath"/> (canonical mapping, ParamName "path"), or it names the method's path
    /// parameter ("sourcePath"/"destinationPath" of two-path operations — the ordinary .NET convention).
    /// </summary>
    public static bool IdentifiesPathArgument(ArgumentException ex) =>
        DfsErrors.ToCode(ex) == DfsErrorCode.InvalidPath || ex.ParamName is "path" or "sourcePath" or "destinationPath";

    /// <summary>
    /// For every bad path and every path-taking entry point: the call fails with an <see cref="ArgumentException"/>
    /// that identifies the path (<see cref="IdentifiesPathArgument"/>; any <see cref="ArgumentNullException"/> for a null path),
    /// no request leaves the client (the message log is unchanged, also after a settle delay) and the namespace is
    /// unchanged (same inode count, valid companions intact).
    /// </summary>
    public static async Task AssertRejectedLocallyAsync(TestCluster cluster, PathOperationContext context, IEnumerable<string?> badPaths,
        string clientNetworkName = "client:c0")
    {
        int inodesBefore = await SdkHelpers.InodeCountAsync(cluster);
        long requestsBefore = SdkHelpers.ForegroundRequests(cluster, clientNetworkName);
        var failures = new List<string>();
        int checkedCalls = 0;
        foreach (string? bad in badPaths)
        {
            foreach (PathOperation op in Operations)
            {
                checkedCalls++;
                long before = SdkHelpers.ForegroundRequests(cluster, clientNetworkName);
                Exception? ex = null;
                try
                {
                    await op.Invoke(context, bad!);
                }
                catch (Exception e)
                {
                    ex = SdkHelpers.Unwrap(e);
                }

                string call = $"{op.Name}({Describe(bad)})";
                if (ex is null)
                    failures.Add($"{call} completed instead of rejecting the path");
                else if (ex is not ArgumentException)
                    failures.Add($"{call} threw {ex.GetType().Name} ({DfsErrors.ToCode(ex)}) instead of ArgumentException: {ex.Message}");
                else if (bad is not null && !IdentifiesPathArgument((ArgumentException)ex))
                    failures.Add($"{call} threw ArgumentException classified {DfsErrors.ToCode(ex)} (ParamName '{((ArgumentException)ex).ParamName}') that does not identify the path: {ex.Message}");
                else if (bad is null && ex is not ArgumentNullException)
                    failures.Add($"{call} threw {ex.GetType().Name} instead of ArgumentNullException for a null path");

                long after = SdkHelpers.ForegroundRequests(cluster, clientNetworkName);
                if (after != before)
                    failures.Add($"{call} sent {after - before} request(s) before rejecting the path");
            }
        }

        // Late deliveries of a wrongly transmitted request would show up after a short settle.
        await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), SdkHelpers.Ct);
        long requestsAfter = SdkHelpers.ForegroundRequests(cluster, clientNetworkName);
        if (requestsAfter != requestsBefore)
            failures.Add($"{requestsAfter - requestsBefore} request(s) reached the cluster during the sweep of {checkedCalls} rejected calls");
        int inodesAfter = await SdkHelpers.InodeCountAsync(cluster);
        if (inodesAfter != inodesBefore)
            failures.Add($"inode count changed from {inodesBefore} to {inodesAfter} during the sweep");
        if (!await context.Files.ExistsAsync(context.ExistingFile, SdkHelpers.Ct))
            failures.Add($"valid companion {context.ExistingFile} disappeared during the sweep");
        if (!await context.Directories.ExistsAsync(context.ExistingDirectory, SdkHelpers.Ct))
            failures.Add($"valid companion {context.ExistingDirectory} disappeared during the sweep");
        if (await context.Files.ExistsAsync(context.FreshTarget, SdkHelpers.Ct) || await context.Directories.ExistsAsync(context.FreshTarget, SdkHelpers.Ct))
            failures.Add($"{context.FreshTarget} was created by a rejected call");

        Assert.True(failures.Count == 0,
            $"{failures.Count} of {checkedCalls} invalid-path calls misbehaved:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
    }

    /// <summary>Printable description of a (possibly malformed or very long) path.</summary>
    public static string Describe(string? path)
    {
        if (path is null)
            return "null";
        var sb = new StringBuilder("\"");
        int shown = 0;
        foreach (char ch in path)
        {
            if (shown++ == 48)
            {
                sb.Append("\u2026");
                break;
            }

            if (ch < 0x20 || ch == '\\' || char.IsSurrogate(ch) || ch > 0x7E)
                sb.Append(CultureInfo.InvariantCulture, $"\\u{(int)ch:X4}");
            else
                sb.Append(ch);
        }

        sb.Append('"');
        if (path.Length > 48)
            sb.Append(CultureInfo.InvariantCulture, $" ({path.Length} chars, {Encoding.UTF8.GetByteCount(path)} UTF-8 bytes)");
        return sb.ToString();
    }

    /// <summary>Exactly <paramref name="bytes"/> UTF-8 bytes of ASCII 'a'.</summary>
    public static string Ascii(int bytes, char ch = 'a') => new(ch, bytes);

    /// <summary>Exactly <paramref name="bytes"/> UTF-8 bytes: 2-byte 'é' (U+00E9) padded with one ASCII letter when odd.</summary>
    public static string TwoByte(int bytes) => new string('\u00E9', bytes / 2) + new string('a', bytes % 2);

    /// <summary>Exactly <paramref name="bytes"/> UTF-8 bytes: 3-byte '€' (U+20AC) padded with ASCII letters.</summary>
    public static string ThreeByte(int bytes) => new string('\u20AC', bytes / 3) + new string('a', bytes % 3);

    /// <summary>Exactly <paramref name="bytes"/> UTF-8 bytes: 4-byte U+1F600 (a surrogate pair) padded with ASCII letters.</summary>
    public static string FourByte(int bytes) => string.Concat(Enumerable.Repeat("\U0001F600", bytes / 4)) + new string('a', bytes % 4);

    /// <summary>
    /// A valid absolute path whose UTF-8 encoding is exactly <paramref name="totalBytes"/> bytes, made of components of
    /// at most 200 bytes produced by <paramref name="filler"/> (which must return exactly the requested byte count).
    /// </summary>
    public static string BuildPath(int totalBytes, Func<int, string> filler)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(totalBytes, 2);
        var sb = new StringBuilder();
        int remaining = totalBytes;
        while (remaining > 0)
        {
            int segment = Math.Min(201, remaining);
            if (remaining - segment is > 0 and < 2)
                segment = remaining - 2; // leave room for a final "/x"
            sb.Append('/').Append(filler(segment - 1));
            remaining -= segment;
        }

        string path = sb.ToString();
        if (Encoding.UTF8.GetByteCount(path) != totalBytes)
            throw new InvalidOperationException($"Path builder produced {Encoding.UTF8.GetByteCount(path)} bytes instead of {totalBytes}.");
        return path;
    }

    /// <summary>The oracle: NFC via ICU (test projects run with full globalization).</summary>
    public static string OracleNfc(string value) => value.Normalize(NormalizationForm.FormC);

    /// <summary>Runs <paramref name="action"/> with <see cref="CultureInfo.CurrentCulture"/> and UI culture set to <paramref name="cultureName"/>.</summary>
    public static async Task WithCultureAsync(string cultureName, Func<Task> action)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo previousUi = CultureInfo.CurrentUICulture;
        CultureInfo culture = CultureInfo.GetCultureInfo(cultureName);
        CultureInfo.CurrentCulture = culture;
        CultureInfo.CurrentUICulture = culture;
        try
        {
            await action();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
            CultureInfo.CurrentUICulture = previousUi;
        }
    }

    /// <summary>Synchronous variant of <see cref="WithCultureAsync"/>.</summary>
    public static T WithCulture<T>(string cultureName, Func<T> action)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo previousUi = CultureInfo.CurrentUICulture;
        CultureInfo culture = CultureInfo.GetCultureInfo(cultureName);
        CultureInfo.CurrentCulture = culture;
        CultureInfo.CurrentUICulture = culture;
        try
        {
            return action();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
            CultureInfo.CurrentUICulture = previousUi;
        }
    }

    /// <summary>Connects a raw protocol client to the leader of <paramref name="volume"/> and authenticates it as the cluster admin.</summary>
    public static async Task<RawProtocolClient> ConnectRawToLeaderAsync(TestCluster cluster, string volume = Volume)
    {
        TestNode leader = await cluster.GetLeaderAsync(volume, SdkHelpers.Ct);
        RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, SdkHelpers.Ct);
        Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
        Assert.True(raw.IsConnected);
        return raw;
    }
}
