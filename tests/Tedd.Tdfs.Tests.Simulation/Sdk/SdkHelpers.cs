using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Simulation.Sdk;

/// <summary>Shared helpers for the SDK (and PATH) simulation-level suites.</summary>
internal static class SdkHelpers
{
    /// <summary>The default volume created by the harness.</summary>
    public const string Volume = "vol";

    /// <summary>Cancellation token of the running test (only passed to <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>).</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The default three-zone cluster in deterministic simulation.</summary>
    public static TestClusterOptions Deterministic => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic };

    private static readonly DfsOperationKind[] BackgroundKinds =
        [DfsOperationKind.KeepAlive, DfsOperationKind.Discover, DfsOperationKind.Authenticate];

    /// <summary>Network identity of a harness client.</summary>
    public static string NetworkName(string clientName) => "client:" + clientName;

    /// <summary>Requests sent by a client other than background keep-alive/discovery/handshake traffic.</summary>
    public static long ForegroundRequests(TestCluster cluster, string clientNetworkName)
    {
        MessageLog log = cluster.Network.Log;
        long total = log.CountMessages(new MessageFilter(From: clientNetworkName, Direction: LinkDirection.Request));
        foreach (DfsOperationKind kind in BackgroundKinds)
            total -= log.CountMessages(new MessageFilter(From: clientNetworkName, Direction: LinkDirection.Request, Operation: kind));
        return total;
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

    /// <summary>Unwraps single-inner aggregate exceptions.</summary>
    public static Exception Unwrap(Exception ex)
    {
        while (ex is AggregateException { InnerExceptions.Count: 1 } agg)
            ex = agg.InnerExceptions[0];
        return ex;
    }

    /// <summary>True for the typed availability failures a connect may legally end with.</summary>
    public static bool IsTypedAvailabilityFailure(Exception ex) =>
        ex is DfsUnavailableException or TimeoutException
        || (ex is DfsException d && d.ErrorCode is DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable or DfsErrorCode.QuorumUnavailable or DfsErrorCode.Timeout);

    /// <summary>Reads a whole file (a separate statement so spans never cross an await).</summary>
    public static Task<byte[]> ReadAsync(DfsVolume volume, string path) => volume.Files.ReadAllBytesAsync(path);

    /// <summary>Writes a line to the test output.</summary>
    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
