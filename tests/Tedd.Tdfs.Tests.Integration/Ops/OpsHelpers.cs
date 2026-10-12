using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Ops;

/// <summary>Shared helpers for the OPS integration tests.</summary>
internal static class OpsHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Three nodes, one per zone, real QUIC transport and on-disk storage.</summary>
    public static TestClusterOptions QuicDisk() => new()
    {
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        ScenarioTimeout = TimeSpan.FromMinutes(3),
    };

    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);
}

/// <summary>
/// Polls every health/readiness/status endpoint back-to-back on a background task until stopped, recording failures. Used
/// to prove that observation has no side effects on authority, leases or durability accounting.
/// </summary>
internal sealed class HealthPoller : IAsyncDisposable
{
    private readonly CancellationTokenSource _stop = new();
    private readonly List<Exception> _errors = [];
    private readonly Lock _lock = new();
    private readonly Task _loop;
    private long _calls;

    public HealthPoller(IReadOnlyList<DfsAdminClient> admins, string volume)
    {
        _loop = Task.Run(() => LoopAsync(admins, volume));
    }

    public long Calls => Interlocked.Read(ref _calls);

    public IReadOnlyList<Exception> Errors
    {
        get
        {
            lock (_lock)
                return _errors.ToArray();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        await _loop;
        _stop.Dispose();
    }

    private async Task LoopAsync(IReadOnlyList<DfsAdminClient> admins, string volume)
    {
        while (!_stop.IsCancellationRequested)
        {
            foreach (DfsAdminClient admin in admins)
            {
                try
                {
                    await admin.GetClusterStatusAsync(_stop.Token);
                    await admin.ListNodesAsync(_stop.Token);
                    await admin.GetVolumeStatusAsync(volume, _stop.Token);
                    await admin.GetReplicaHealthAsync(volume, _stop.Token);
                    Interlocked.Add(ref _calls, 4);
                }
                catch (OperationCanceledException) when (_stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex) when (ex is DfsUnavailableException or TimeoutException)
                {
                    // Availability failures of a health endpoint are legal while a node is paused; they are not side effects.
                }
                catch (Exception ex)
                {
                    lock (_lock)
                        _errors.Add(ex);
                }
            }
        }
    }
}
