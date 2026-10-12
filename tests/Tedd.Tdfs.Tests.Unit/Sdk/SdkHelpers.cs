using System.Collections.Concurrent;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Sdk;

/// <summary>Shared helpers for the SDK (and PATH) unit-level suites.</summary>
internal static class SdkHelpers
{
    /// <summary>The default volume created by the harness.</summary>
    public const string Volume = "vol";

    /// <summary>Cancellation token of the running test.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Operation kinds a connected client may send without any application call (session renewal, routing refresh, handshake).</summary>
    private static readonly DfsOperationKind[] BackgroundKinds =
        [DfsOperationKind.KeepAlive, DfsOperationKind.Discover, DfsOperationKind.Authenticate];

    /// <summary>Network identity of a harness client.</summary>
    public static string NetworkName(string clientName = "c0") => "client:" + clientName;

    /// <summary>
    /// Requests sent by <paramref name="clientNetworkName"/> other than background keep-alive/discovery/handshake
    /// traffic: every request that an application call could have caused.
    /// </summary>
    public static long ForegroundRequests(TestCluster cluster, string clientNetworkName)
    {
        MessageLog log = cluster.Network.Log;
        long total = log.CountMessages(new MessageFilter(From: clientNetworkName, Direction: LinkDirection.Request));
        foreach (DfsOperationKind kind in BackgroundKinds)
            total -= log.CountMessages(new MessageFilter(From: clientNetworkName, Direction: LinkDirection.Request, Operation: kind));
        return total;
    }

    /// <summary>Requests of one operation kind sent by a client.</summary>
    public static long RequestsOfKind(TestCluster cluster, string clientNetworkName, DfsOperationKind kind) =>
        cluster.Network.Log.CountMessages(new MessageFilter(From: clientNetworkName, Direction: LinkDirection.Request, Operation: kind));

    /// <summary>Number of inodes in the leader's applied state of <paramref name="volume"/>.</summary>
    public static async Task<int> InodeCountAsync(TestCluster cluster, string volume = Volume) =>
        (await cluster.Inspect.GetVolumeStateAsync(volume)).InodeCount;

    /// <summary>Names (not paths) of the entries of a directory, in enumeration order.</summary>
    public static async Task<List<string>> ListNamesAsync(DfsDirectoryClient directories, string path)
    {
        var names = new List<string>();
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(path, null, Ct))
            names.Add(entry.Name);
        return names;
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts it equals <paramref name="expected"/> byte for byte (first mismatch reported).</summary>
    public static async Task AssertBytesEqualAsync(ReadOnlyMemory<byte> expected, Task<byte[]> actual, string? because = null)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected.Span, bytes, because);
    }

    /// <summary>Runs <paramref name="action"/> and returns the exception it threw; fails when it completes normally.</summary>
    public static async Task<Exception> CaptureAsync(Func<Task> action, string because)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            return ex;
        }

        Assert.Fail($"Expected an exception but the call completed normally: {because}");
        throw new InvalidOperationException("unreachable");
    }

    /// <summary>Unwraps a single-inner <see cref="AggregateException"/> or <see cref="System.Reflection.TargetInvocationException"/>.</summary>
    public static Exception Unwrap(Exception ex)
    {
        while (true)
        {
            switch (ex)
            {
                case AggregateException { InnerExceptions.Count: 1 } agg:
                    ex = agg.InnerExceptions[0];
                    continue;
                case System.Reflection.TargetInvocationException { InnerException: not null } tie:
                    ex = tie.InnerException!;
                    continue;
                default:
                    return ex;
            }
        }
    }

    /// <summary>Full textual rendering of an exception chain including <see cref="Exception.Data"/> values.</summary>
    public static string Render(Exception ex)
    {
        var sb = new System.Text.StringBuilder();
        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            sb.Append(e.GetType().FullName).Append(": ").Append(e.Message).AppendLine();
            foreach (System.Collections.DictionaryEntry entry in e.Data)
                sb.Append("  data ").Append(entry.Key).Append('=').Append(entry.Value).AppendLine();
            if (e is AggregateException agg)
            {
                foreach (Exception inner in agg.InnerExceptions)
                    sb.Append(Render(inner));
            }
        }

        sb.Append(ex);
        return sb.ToString();
    }

    /// <summary>Writes a line to the test output.</summary>
    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>
    /// Reads a whole file through a live read stream opened with <see cref="FileShare.ReadWrite"/> |
    /// <see cref="FileShare.Delete"/>, so the read coexists with writers and transaction reservations that are still
    /// active on the file (test plan §5: visibility checks use compatible sharing options; a setup-level sharing
    /// conflict is not evidence about publication).
    /// </summary>
    public static async Task<byte[]> ReadSharedAsync(DfsFileClient files, string path, CancellationToken ct = default)
    {
        await using DfsStream stream = await files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, ct);
        var copy = new MemoryStream();
        await stream.CopyToAsync(copy, ct);
        return copy.ToArray();
    }

    /// <summary>
    /// Runs <paramref name="action"/> and retries it (bounded, with a short backoff on the cluster clock) while it fails
    /// with a <see cref="DfsSharingViolationException"/>. A sharing/writer conflict is a definite rejection without
    /// effect (LOCK-03: no automatic waiting), so wrapping the retries inside one recorded history operation keeps the
    /// history legal: the effect still lies within the operation's invocation/completion window.
    /// </summary>
    public static async Task<T> RetryOnSharingViolationAsync<T>(TestCluster cluster, Func<Task<T>> action, int maxAttempts = 200)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await action();
            }
            catch (DfsSharingViolationException) when (attempt < maxAttempts)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(5 + (attempt % 8) * 5));
            }
        }
    }
}

/// <summary>Records every SDK diagnostic event (thread-safe).</summary>
internal sealed class RecordingDiagnostics : IDfsDiagnostics
{
    private readonly ConcurrentQueue<DfsDiagnosticEvent> _events = new();

    /// <summary>All events recorded so far.</summary>
    public IReadOnlyList<DfsDiagnosticEvent> Events => _events.ToArray();

    /// <inheritdoc />
    public void OnEvent(DfsDiagnosticEvent e) => _events.Enqueue(e);
}
