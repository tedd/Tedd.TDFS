using System.Security.Cryptography;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// One authenticated connection of the client to a node (live or backup): the transport connection plus the handshake
/// outcome (PROTOCOL.md §3). Requests are multiplexed by the transport; a connection is never shared between clients.
/// </summary>
/// <remarks>
/// A call waits for its reply for its attempt timeout, but the transport keeps waiting for a late reply for the call's
/// liveness horizon (<see cref="LivenessHorizon"/>): a late reply still updates the endpoint's round-trip estimate
/// (<see cref="Rtt"/>) and shows that the connection is alive, merely slow. The connection is declared dead (and removed
/// from the pool) only when a call's whole horizon passed without any reply on the connection since the call was sent
/// (silently dead links, NET-016), never because one short attempt (a keep-alive, a lease-granting request) timed out on a
/// link whose latency grew (NET-013).
/// </remarks>
internal sealed class ClientConnection
{
    private readonly ClientRuntime _runtime;
    private long _abandonedBytes;
    private int _poisoned;
    private int _closed;
    private long _lastReply;

    private ClientConnection(ClientRuntime runtime, IRpcConnection raw, string endpoint, HelloChallengeMessage challenge, HelloResultMessage result,
        long openedAt, RttEstimator rtt)
    {
        _runtime = runtime;
        _lastReply = openedAt;
        Rtt = rtt;
        Raw = raw;
        Endpoint = endpoint;
        Challenge = challenge;
        Result = result;
        raw.Closed += (_, _) => Volatile.Write(ref _poisoned, 1);
    }

    /// <summary>Transport connection.</summary>
    public IRpcConnection Raw { get; }

    /// <summary>Endpoint the connection was opened to.</summary>
    public string Endpoint { get; }

    /// <summary>The server's challenge (cluster identity, server identity, negotiated version and features).</summary>
    public HelloChallengeMessage Challenge { get; }

    /// <summary>The accepting handshake result (principal, cluster permissions, capabilities).</summary>
    public HelloResultMessage Result { get; }

    /// <summary>Node id of the server, when it reported one.</summary>
    public NodeId? ServerNodeId => Challenge.ServerNodeId;

    /// <summary>True when the server is a read-only backup host.</summary>
    public bool IsBackupHost => Challenge.ServerRole == PeerRole.Backup;

    /// <summary>False once the transport closed or the connection was declared dead (stale pooled connection).</summary>
    public bool IsUsable => Volatile.Read(ref _poisoned) == 0 && Raw.IsOpen;

    /// <summary>Time-provider timestamp of the last reply received on this connection (initially its handshake).</summary>
    public long LastReply => Volatile.Read(ref _lastReply);

    /// <summary>
    /// The round-trip estimate of the endpoint (shared with its earlier and later connections), fed by every reply on this
    /// connection, late ones included: how long a slow but answering server takes, so that waits are not cut below it.
    /// </summary>
    public RttEstimator Rtt { get; }

    /// <summary>Smoothed round-trip time of the endpoint (zero while unknown).</summary>
    public TimeSpan RoundTrip => Rtt.Smoothed;

    /// <summary>
    /// <paramref name="configured"/> attempt timeout raised to the endpoint's adaptive reply timeout (a few smoothed round
    /// trips plus their deviation), never past <paramref name="limit"/> (what the operation has left for the attempt).
    /// </summary>
    public TimeSpan AdaptAttemptTimeout(TimeSpan configured, TimeSpan limit) => Rtt.Adapt(configured, limit);

    /// <summary>
    /// How long a call with <paramref name="attemptTimeout"/> keeps waiting for a late reply before its silence counts as
    /// evidence that the connection is dead: one and a half attempt timeouts, at least two adaptive reply timeouts, and at
    /// least the default patience of a single request (a third of the operation timeout) — a short attempt gives up early
    /// to fail over, but does not condemn a connection that a default request would still be waiting on.
    /// </summary>
    public TimeSpan LivenessHorizon(TimeSpan attemptTimeout)
    {
        TimeSpan horizon = attemptTimeout + (attemptTimeout / 2);
        TimeSpan adaptive = Rtt.ReplyTimeout * 2;
        if (adaptive > horizon)
            horizon = adaptive;
        TimeSpan patience = TimeSpan.FromTicks(Math.Max(_runtime.Options.OperationTimeout.Ticks / 3, TimeSpan.FromMilliseconds(100).Ticks));
        return patience > horizon ? patience : horizon;
    }

    /// <summary>
    /// Bytes of requests this connection still carries that nobody waits for any more (their caller timed out or stopped
    /// waiting, for example an upload to a replica that was not needed once the policy was met elsewhere): what the link
    /// may still be busy with before a new request gets through. Uploads use it to apply backpressure to a congested target.
    /// </summary>
    public long AbandonedBytes => Interlocked.Read(ref _abandonedBytes);

    /// <summary>Records that a reply arrived on this connection at <paramref name="timestamp"/>.</summary>
    private void NoteReply(long timestamp)
    {
        long current = Volatile.Read(ref _lastReply);
        while (timestamp > current)
        {
            long seen = Interlocked.CompareExchange(ref _lastReply, timestamp, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>
    /// Sends <paramref name="frame"/> and decodes the reply (see <see cref="StartCall"/>). Transport failures
    /// (<see cref="TimeoutException"/>, <see cref="RpcConnectionException"/>) and cancellation propagate unchanged.
    /// </summary>
    public async Task<IWireMessage> CallAsync(ReadOnlyMemory<byte> frame, TimeSpan timeout, CancellationToken ct)
    {
        ReadOnlyMemory<byte> reply = await StartCall(frame, timeout, ct).ConfigureAwait(false);
        return MessageCodec.Decode(reply.Span);
    }

    /// <summary>
    /// Starts a call whose reply is awaited for <paramref name="timeout"/> (then <see cref="TimeoutException"/>) or until
    /// <paramref name="ct"/> fires; the returned task carries the raw reply (used when the caller must know whether it was
    /// sent: a call refused before sending completes faulted with <see cref="RpcConnectionException"/> at once). The
    /// transport keeps waiting for a late reply up to <see cref="LivenessHorizon"/> after the caller stopped waiting. When
    /// <paramref name="extendUpTo"/> is given and the endpoint's round-trip estimate grew while the call was waiting (a late
    /// reply of an earlier call showed the link became slow), the wait is extended to the new adaptive reply timeout, up to
    /// that bound, instead of giving up on a reply that is merely slow.
    /// </summary>
    public Task<ReadOnlyMemory<byte>> StartCall(ReadOnlyMemory<byte> frame, TimeSpan timeout, CancellationToken ct, TimeSpan? extendUpTo = null)
    {
        if (ct.IsCancellationRequested)
            return Task.FromCanceled<ReadOnlyMemory<byte>>(ct);
        TimeProvider time = _runtime.Time;
        long sentAt = time.GetTimestamp();
        TimeSpan horizon = LivenessHorizon(timeout);
        Task<ReadOnlyMemory<byte>> raw = Raw.CallAsync(frame, horizon, CancellationToken.None);
        if (raw.IsCompleted)
        {
            Observe(raw, sentAt);
            return raw;
        }

        _ = raw.ContinueWith(static (t, s) =>
        {
            (ClientConnection connection, long sent) = ((ClientConnection, long))s!;
            connection.Observe(t, sent);
        }, (this, sentAt), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        var wait = new AttemptWait(this, raw, frame.Length, ct, sentAt, timeout, extendUpTo is { } bound && bound > timeout ? bound : timeout,
            $"No reply from {Endpoint} within {timeout}; the request may have been executed.");
        wait.Start(time);
        return wait.Task;
    }

    /// <summary>
    /// Learns from a finished transport call: a reply (in time or late) is a round-trip sample and proof of life; a call
    /// whose whole liveness horizon passed with no reply on the connection since it was sent declares the connection dead.
    /// </summary>
    private void Observe(Task<ReadOnlyMemory<byte>> raw, long sentAt)
    {
        TimeProvider time = _runtime.Time;
        if (raw.IsCompletedSuccessfully)
        {
            long now = time.GetTimestamp();
            NoteReply(now);
            Rtt.Sample(time.GetElapsedTime(sentAt, now), now);
            return;
        }

        Exception? error = raw.Exception?.GetBaseException();
        if (error is TimeoutException && LastReply < sentAt && IsUsable)
        {
            _runtime.Emit("connection.silent", $"No reply on the connection to {Endpoint} for {time.GetElapsedTime(sentAt)}: it is declared dead and replaced.");
            _runtime.Pool.Invalidate(this);
        }
    }

    /// <summary>
    /// The caller's wait for one call: completed by the call, by the attempt timer (<see cref="TimeoutException"/>; the call
    /// keeps running) or by the caller's token, always without a synchronization context (ARCHITECTURE §9.6). When the
    /// timer fires while the round-trip estimate now asks for a longer wait (up to the extension bound), it is re-armed.
    /// </summary>
    private sealed class AttemptWait
    {
        private readonly TaskCompletionSource<ReadOnlyMemory<byte>> _source = new(TaskCreationOptions.None);
        private readonly ClientConnection _connection;
        private readonly Task<ReadOnlyMemory<byte>> _call;
        private readonly int _bytes;
        private readonly CancellationToken _ct;
        private readonly long _sentAt;
        private readonly TimeSpan _timeout;
        private readonly TimeSpan _extendUpTo;
        private readonly string _timeoutMessage;
        private TimeProvider? _time;
        private ITimer? _timer;
        private CancellationTokenRegistration _registration;
        private int _done;
        private int _phase; // 0: waited for, 1: abandoned (counted in the connection's abandoned bytes), 2: the call finished

        public AttemptWait(ClientConnection connection, Task<ReadOnlyMemory<byte>> call, int bytes, CancellationToken ct, long sentAt, TimeSpan timeout,
            TimeSpan extendUpTo, string timeoutMessage)
        {
            _connection = connection;
            _call = call;
            _bytes = bytes;
            _ct = ct;
            _sentAt = sentAt;
            _timeout = timeout;
            _extendUpTo = extendUpTo;
            _timeoutMessage = timeoutMessage;
        }

        public Task<ReadOnlyMemory<byte>> Task => _source.Task;

        public void Start(TimeProvider time)
        {
            _time = time;
            _timer = time.CreateTimer(static s => ((AttemptWait)s!).OnTimer(), this, ClientRuntime.Positive(_timeout), Timeout.InfiniteTimeSpan);
            if (_ct.CanBeCanceled)
                _registration = _ct.UnsafeRegister(static s => ((AttemptWait)s!).Finish(Outcome.Cancelled), this);
            _ = _call.ContinueWith(static (_, s) => ((AttemptWait)s!).OnCallFinished(), this, CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            if (Volatile.Read(ref _done) != 0)
                Cleanup();
        }

        private void OnTimer()
        {
            if (Volatile.Read(ref _done) != 0)
                return;
            if (_extendUpTo > _timeout && _time is { } time && _timer is { } timer)
            {
                TimeSpan wanted = _connection.Rtt.Adapt(_timeout, _extendUpTo);
                TimeSpan left = wanted - time.GetElapsedTime(_sentAt);
                if (left > TimeSpan.FromMilliseconds(1))
                {
                    try
                    {
                        timer.Change(left, Timeout.InfiniteTimeSpan);
                        return;
                    }
                    catch (ObjectDisposedException)
                    {
                        return; // finished meanwhile
                    }
                }
            }

            Finish(Outcome.TimedOut);
        }

        private void OnCallFinished()
        {
            if (Interlocked.Exchange(ref _phase, 2) == 1)
                Interlocked.Add(ref _connection._abandonedBytes, -_bytes);
            Finish(Outcome.Completed);
        }

        private void Finish(Outcome outcome)
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
                return;
            Cleanup();
            if (outcome != Outcome.Completed && Interlocked.CompareExchange(ref _phase, 1, 0) == 0)
                Interlocked.Add(ref _connection._abandonedBytes, _bytes);
            ContextFree.Run(static p =>
            {
                (AttemptWait wait, Outcome how) = p;
                switch (how)
                {
                    case Outcome.TimedOut:
                        wait._source.TrySetException(new TimeoutException(wait._timeoutMessage));
                        break;
                    case Outcome.Cancelled:
                        wait._source.TrySetCanceled(wait._ct);
                        break;
                    default:
                        Task<ReadOnlyMemory<byte>> call = wait._call;
                        if (call.IsCompletedSuccessfully)
                            wait._source.TrySetResult(call.Result);
                        else if (call.IsCanceled)
                            wait._source.TrySetCanceled(wait._ct.IsCancellationRequested ? wait._ct : CancellationToken.None);
                        else
                            wait._source.TrySetException(call.Exception!.InnerExceptions);
                        break;
                }
            }, (this, outcome));
        }

        private void Cleanup()
        {
            _timer?.Dispose();
            _registration.Unregister();
        }

        private enum Outcome
        {
            Completed,
            TimedOut,
            Cancelled,
        }
    }

    /// <summary>Declares the connection dead (silently dead links, protocol violations) and closes it in the background.</summary>
    public void Poison()
    {
        Volatile.Write(ref _poisoned, 1);
        if (Interlocked.Exchange(ref _closed, 1) == 0)
            CloseQuietly(Raw);
    }

    /// <summary>Closes a transport connection, ignoring failures (the connection is being discarded).</summary>
    public static void CloseQuietly(IRpcConnection connection)
    {
        ValueTask close;
        try
        {
            close = connection.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            return;
        }

        if (!close.IsCompletedSuccessfully)
            Waits.ObserveQuietly(close.AsTask());
    }

    /// <summary>
    /// Opens a transport connection to <paramref name="endpoint"/> and authenticates the client principal
    /// (Hello → HelloChallenge → HelloProof → HelloResult). The cluster identity is checked before any proof is sent and
    /// the server's mutual-authentication MAC before the connection is returned. Failures close the connection.
    /// </summary>
    /// <exception cref="DfsClusterIdentityException">The node belongs to another cluster.</exception>
    /// <exception cref="DfsAuthenticationException">Authentication failed in either direction.</exception>
    /// <param name="runtime">The client runtime.</param>
    /// <param name="endpoint">The node's endpoint.</param>
    /// <param name="expectedClusterId">The cluster the node must belong to (empty: any).</param>
    /// <param name="timeout">Longest wait for each handshake reply.</param>
    /// <param name="rtt">The endpoint's round-trip estimate (the handshake's round trips are samples of it).</param>
    /// <param name="ct">Cancels the connect.</param>
    public static async Task<ClientConnection> OpenAsync(ClientRuntime runtime, string endpoint, Guid expectedClusterId, TimeSpan timeout, RttEstimator rtt,
        CancellationToken ct)
    {
        IRpcConnection raw = await runtime.Transport.ConnectAsync(endpoint, ct).ConfigureAwait(false);
        try
        {
            return await AuthenticateAsync(runtime, raw, endpoint, expectedClusterId, timeout, rtt, ct).ConfigureAwait(false);
        }
        catch
        {
            CloseQuietly(raw);
            throw;
        }
    }

    /// <summary>Client wire stages of the handshake (F03/F14, operation <c>Authenticate</c>; occurrence 0 = Hello, 1 = proof).</summary>
    private static ValueTask HitAsync(ClientRuntime runtime, FaultStage stage, string site, string endpoint, int occurrence, CancellationToken ct) =>
        runtime.Faults.IsEnabled
            ? runtime.Faults.HitAsync(new FaultContext(stage, site, runtime.NetworkName, null, DfsOperationKind.Authenticate, Occurrence: occurrence,
                Detail: endpoint), ct)
            : ValueTask.CompletedTask;

    private static async Task<ClientConnection> AuthenticateAsync(ClientRuntime runtime, IRpcConnection raw, string endpoint, Guid expectedClusterId,
        TimeSpan timeout, RttEstimator rtt, CancellationToken ct)
    {
        DfsCredentials credentials = runtime.Options.Credentials;
        var hello = new HelloMessage(expectedClusterId, ProtocolInfo.MinSupportedVersion, ProtocolInfo.CurrentVersion, ClientRuntime.SupportedFeatures,
            PeerRole.Client, runtime.Options.ClientName, null, 0, credentials.Principal, runtime.Random.NextBytes(ProtocolInfo.NonceSize));
        byte[] helloFrame = MessageCodec.Encode(hello);
        await HitAsync(runtime, FaultStage.F03, FaultSites.ClientRequestSend, endpoint, 0, ct).ConfigureAwait(false);
        long helloSentAt = runtime.Time.GetTimestamp();
        ReadOnlyMemory<byte> challengeReply = await raw.CallAsync(helloFrame, timeout, ct).ConfigureAwait(false);
        long challengeAt = runtime.Time.GetTimestamp();
        rtt.Sample(runtime.Time.GetElapsedTime(helloSentAt, challengeAt), challengeAt);
        await HitAsync(runtime, FaultStage.F14, FaultSites.ClientReplyReceived, endpoint, 0, ct).ConfigureAwait(false);
        IWireMessage challengeMessage = MessageCodec.Decode(challengeReply.Span);
        if (challengeMessage is ErrorReplyMessage error)
            throw error.ToException();
        if (challengeMessage is not HelloChallengeMessage challenge)
            throw new DfsProtocolException($"Expected HelloChallenge from {endpoint}, received {challengeMessage.Type}.");
        if (expectedClusterId != Guid.Empty && challenge.ClusterId != expectedClusterId)
        {
            throw new DfsClusterIdentityException(
                $"The node at {endpoint} belongs to cluster {challenge.ClusterId}, not the expected cluster {expectedClusterId}.");
        }

        if (challenge.ProtocolVersion < ProtocolInfo.MinSupportedVersion || challenge.ProtocolVersion > ProtocolInfo.CurrentVersion)
        {
            throw new DfsProtocolException(
                $"The node at {endpoint} selected protocol version {challenge.ProtocolVersion}; this SDK supports {ProtocolInfo.MinSupportedVersion}..{ProtocolInfo.CurrentVersion}.");
        }

        if (challenge.Pbkdf2Iterations < 1 || challenge.Pbkdf2Iterations > HandshakeAuth.MaxIterations)
            throw new DfsAuthenticationException($"The node at {endpoint} asked for an unacceptable key-derivation iteration count.");

        byte[] transcript = HandshakeAuth.ComputeTranscript(helloFrame, challengeReply.Span);
        ClientKeys keys = runtime.GetClientKeys(challenge.PrincipalSalt.Span, challenge.Pbkdf2Iterations);
        byte[] proof = HandshakeAuth.ComputeClientProof(keys, transcript);
        await HitAsync(runtime, FaultStage.F03, FaultSites.ClientRequestSend, endpoint, 1, ct).ConfigureAwait(false);
        long proofSentAt = runtime.Time.GetTimestamp();
        ReadOnlyMemory<byte> resultReply = await raw.CallAsync(MessageCodec.Encode(new HelloProofMessage(proof)), timeout, ct).ConfigureAwait(false);
        long resultAt = runtime.Time.GetTimestamp();
        rtt.Sample(runtime.Time.GetElapsedTime(proofSentAt, resultAt), resultAt);
        await HitAsync(runtime, FaultStage.F14, FaultSites.ClientReplyReceived, endpoint, 1, ct).ConfigureAwait(false);
        CryptographicOperations.ZeroMemory(proof);
        IWireMessage resultMessage = MessageCodec.Decode(resultReply.Span);
        if (resultMessage is ErrorReplyMessage resultError)
            throw resultError.ToException();
        if (resultMessage is not HelloResultMessage result)
            throw new DfsProtocolException($"Expected HelloResult from {endpoint}, received {resultMessage.Type}.");
        if (!result.Accepted)
        {
            DfsErrorCode code = result.Error == DfsErrorCode.None ? DfsErrorCode.AuthenticationFailed : result.Error;
            string message = string.IsNullOrEmpty(result.Message) ? "The node rejected the handshake." : result.Message;
            throw DfsErrors.ToException(code, $"{message} (node {endpoint})");
        }

        if (!HandshakeAuth.VerifyServerMac(keys, transcript, result.ServerMac.Span))
            throw new DfsAuthenticationException($"The node at {endpoint} failed mutual authentication.");

        raw.Identity = new RpcPeerIdentity(challenge.ServerName, challenge.ServerNodeId, challenge.ServerIncarnation, null,
            IsNode: challenge.ServerRole == PeerRole.Node, IsBackup: challenge.ServerRole == PeerRole.Backup);
        return new ClientConnection(runtime, raw, endpoint, challenge, result, runtime.Time.GetTimestamp(), rtt);
    }
}
