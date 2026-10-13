using System.Globalization;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// The harness's own administrative channel to a cluster: an authenticated principal connection ("admin") that sends
/// <see cref="AdminRequestMessage"/>s to the control-group leader. It does not depend on the SDK, so the harness can
/// bootstrap pools, volumes, principals and groups through the production admin path (the same handlers, validation,
/// deduplication and Raft commit as an administrator's request) before and independently of any SDK client.
/// </summary>
/// <remarks>
/// Requests are routed to the control leader (found through the in-process node inspectors, else by trying the nodes and
/// following <see cref="DfsErrorCode.NotLeader"/> hints). Transient refusals (no leader, unavailable, timeouts, lost
/// connections) are retried with the <b>same</b> request id, so a retried mutation is deduplicated by the leader exactly
/// like an SDK retry. Its traffic is marked as harness traffic (excluded from <see cref="TestNetwork.CountAttemptsTo"/>).
/// Never uses a cancellable delay or a context-escaping await (simulation-compatible).
/// </remarks>
internal sealed class HarnessAdmin : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly TestCluster _cluster;
    private readonly Guid _clusterId;
    private readonly DfsCredentials _credentials;
    private readonly Func<int, Task<string?>> _findLeader;
    private readonly IRpcTransportFactory _factory;
    private readonly IRandomSource _random;
    private readonly string _name;
    private IRpcTransport? _transport;
    private (string Endpoint, IRpcConnection Connection)? _cached;
    private bool _disposed;

    /// <summary>Creates a channel.</summary>
    /// <param name="cluster">Cluster whose runtime, fabric and timings the channel uses (its traffic is that cluster's harness traffic).</param>
    /// <param name="name">Network identity of the channel (unique on the fabric).</param>
    /// <param name="clusterId">Cluster the addressed nodes must belong to.</param>
    /// <param name="credentials">Administrator credentials.</param>
    /// <param name="findLeader">Returns the endpoint to try for attempt <c>n</c> (the control leader when known), or null.</param>
    public HarnessAdmin(TestCluster cluster, string name, Guid clusterId, DfsCredentials credentials, Func<int, Task<string?>> findLeader)
    {
        _cluster = cluster;
        _name = name;
        _clusterId = clusterId;
        _credentials = credentials;
        _findLeader = findLeader;
        _random = cluster.ForkRandom("harness-admin/" + name);
        _factory = cluster.Fabric.CreateFactory(new TransportIdentity
        {
            Owner = cluster,
            Clock = cluster.Runtime.Time,
            IsHarness = true,
            Credentials = credentials,
            ClusterId = clusterId,
            Certificate = null,
            TrustedPins = cluster.TrustedPins,
        });
    }

    /// <summary>Default total time one request may take, retries included (harness time).</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Sends one administrative request and returns its result; throws the mapped exception for a definite failure.</summary>
    /// <exception cref="DfsException">The control leader refused the request (definite failure).</exception>
    /// <exception cref="TimeoutException">No definite answer within <see cref="RequestTimeout"/>.</exception>
    public async Task<AdminResultMessage> CallAsync<TBody>(AdminOp op, TBody body, CancellationToken ct = default, TimeSpan? timeout = null)
        where TBody : IWireRecord<TBody>
    {
        TimeSpan budget = timeout ?? RequestTimeout;
        UInt128 requestId = NewRequestId();
        byte[] frame = MessageCodec.Encode(AdminRequestMessage.Create(op, requestId, body));
        long start = _cluster.Time.GetTimestamp();
        TimeSpan perCall = TimeSpan.FromSeconds(5);
        string? hint = null;
        Exception? last = null;
        int attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (attempt++ > 0)
            {
                if (_cluster.Time.GetElapsedTime(start) > budget)
                {
                    throw new TimeoutException(string.Create(CultureInfo.InvariantCulture,
                        $"Harness admin request {op} got no definite answer within {budget} ({attempt - 1} attempts): {last?.Message}"), last);
                }

                await _cluster.Runtime.DelayAsync(RetryDelay(attempt));
            }

            string? endpoint = hint ?? await _findLeader(attempt);
            hint = null;
            if (endpoint is null)
            {
                last = new DfsUnavailableException(DfsErrorCode.QuorumUnavailable, "No running node can be asked.");
                continue;
            }

            IRpcConnection connection;
            try
            {
                connection = await GetConnectionAsync(endpoint, perCall);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                last = ex;
                continue;
            }

            ReadOnlyMemory<byte> replyFrame;
            try
            {
                replyFrame = await connection.CallAsync(frame, perCall, CancellationToken.None);
            }
            catch (Exception ex) when (ex is TimeoutException or RpcConnectionException or ObjectDisposedException or IOException)
            {
                last = ex;
                await DropAsync(connection);
                continue;
            }

            IWireMessage reply = MessageCodec.Decode(replyFrame.Span);
            switch (reply)
            {
                case AdminResultMessage result:
                    return result;
                case ErrorReplyMessage error when IsRetryable(error):
                    last = error.ToException();
                    hint = error.LeaderHint;
                    if (error.Code is DfsErrorCode.NotLeader or DfsErrorCode.NodeUnavailable or DfsErrorCode.AuthenticationFailed)
                        await DropAsync(connection);
                    continue;
                case ErrorReplyMessage error:
                    throw error.ToException();
                default:
                    throw new DfsProtocolException($"Unexpected reply {reply.Type} to an administrative request.");
            }
        }
    }

    /// <summary>Sends a request whose result body is <typeparamref name="TResult"/>.</summary>
    public async Task<TResult> CallAsync<TBody, TResult>(AdminOp op, TBody body, CancellationToken ct = default, TimeSpan? timeout = null)
        where TBody : IWireRecord<TBody> where TResult : IWireRecord<TResult>
    {
        AdminResultMessage result = await CallAsync(op, body, ct, timeout);
        return result.ReadBody<TResult>();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        IRpcTransport? transport;
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            transport = _transport;
            _transport = null;
            _cached = null;
        }

        if (transport is null)
            return;
        try
        {
            await transport.DisposeAsync();
        }
        catch (Exception ex) when (ex is ObjectDisposedException or RpcConnectionException or IOException)
        {
            // Already gone.
        }
    }

    private UInt128 NewRequestId()
    {
        lock (_gate)
            return new UInt128(_random.NextUInt64(), _random.NextUInt64() | 1);
    }

    private TimeSpan RetryDelay(int attempt)
    {
        TimeSpan unit = _cluster.Options.Timings.Tick * Math.Max(1, _cluster.Options.Timings.HeartbeatTicks);
        return unit * Math.Min(8, attempt);
    }

    private static bool IsRetryable(ErrorReplyMessage error) =>
        error.Retryable || error.Code is DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable or DfsErrorCode.Unavailable
            or DfsErrorCode.NodeUnavailable or DfsErrorCode.Timeout or DfsErrorCode.OutcomeUnknown;

    private static bool IsTransient(Exception ex) => ex is DfsException dfs
        ? dfs.ErrorCode is DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.Timeout or DfsErrorCode.NotLeader
            or DfsErrorCode.QuorumUnavailable
        : ex is TimeoutException or RpcConnectionException or IOException or ObjectDisposedException;

    private async Task<IRpcConnection> GetConnectionAsync(string endpoint, TimeSpan timeout)
    {
        lock (_gate)
        {
            if (_cached is { } cached && string.Equals(cached.Endpoint, endpoint, StringComparison.Ordinal) && cached.Connection.IsOpen)
                return cached.Connection;
        }

        IRpcTransport transport;
        IRandomSource helloRandom;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            transport = _transport ??= _factory.Create(_name, string.Empty);
            helloRandom = _random.Fork("hello/" + _random.NextUInt64().ToString("x16", CultureInfo.InvariantCulture));
        }

        IRpcConnection connection = await transport.ConnectAsync(endpoint, CancellationToken.None);
        try
        {
            await HandshakeClient.AuthenticatePrincipalAsync(connection, _clusterId, _name, _credentials, helloRandom, timeout,
                PeerRole.Client, CancellationToken.None);
        }
        catch
        {
            await DropAsync(connection);
            throw;
        }

        (string, IRpcConnection)? previous;
        lock (_gate)
        {
            previous = _cached;
            _cached = (endpoint, connection);
        }

        if (previous is { } old && !ReferenceEquals(old.Item2, connection))
            await DropAsync(old.Item2);
        return connection;
    }

    private async Task DropAsync(IRpcConnection connection)
    {
        lock (_gate)
        {
            if (_cached is { } cached && ReferenceEquals(cached.Connection, connection))
                _cached = null;
        }

        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception ex) when (ex is ObjectDisposedException or RpcConnectionException or IOException)
        {
            // Already gone.
        }
    }
}
