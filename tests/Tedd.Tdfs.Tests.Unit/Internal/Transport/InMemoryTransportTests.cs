using System.Collections.Concurrent;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Internal.Transport.TransportTestData;

namespace Tedd.Tdfs.Tests.Unit.Internal.Transport;

/// <summary>Behaviour of the realtime in-memory transport (<see cref="InMemoryNetwork"/>).</summary>
public sealed class InMemoryTransportTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<IRpcTransport> StartAsync(InMemoryNetwork network, string name, IRpcHandler handler)
    {
        IRpcTransport transport = network.Factory.Create(name, "mem://" + name);
        await transport.StartAsync(handler, Ct);
        return transport;
    }

    [Fact]
    public async Task Call_ReturnsTheHandlersReply()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", new DelegateHandler((_, request, _, _) =>
            ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray().Reverse().ToArray())));
        await using IRpcTransport client = network.Factory.Create("client:c0", "mem://client-c0");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        ReadOnlyMemory<byte> reply = await connection.CallAsync(new byte[] { 1, 2, 3 }, Soon, Ct);

        Assert.Equal(new byte[] { 3, 2, 1 }, reply.ToArray());
        Assert.True(connection.IsOpen);
        Assert.Equal("mem://n0", connection.Remote.ToString());
        Assert.Equal("mem://n0", server.LocalEndpoint);
    }

    [Fact]
    public async Task SendAsync_DeliversOneWayMessages_WithoutReply()
    {
        var network = new InMemoryNetwork();
        var handler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(new byte[] { 9 }));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        await connection.SendAsync(Frame(100, 1), Ct);
        await WaitUntilAsync(() => handler.Count == 1, "one-way delivery");

        Assert.True(handler.Received.TryPeek(out var received));
        Assert.False(received.ExpectsReply);
        Assert.Equal(Frame(100, 1), received.Payload);
    }

    [Fact]
    public async Task Delivery_IsAsynchronous_NeverOnTheSendersStack()
    {
        var network = new InMemoryNetwork();
        using var gate = new ManualResetEventSlim(false);
        var handler = new DelegateHandler((_, _, _, _) =>
        {
            gate.Wait(Soon); // blocks the delivering thread, never the sender
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);
        });
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        int senderThread = Environment.CurrentManagedThreadId;
        ValueTask send = connection.SendAsync(Frame(1, 1), Ct);
        Assert.True(send.IsCompletedSuccessfully);
        await send;
        gate.Set();
        await WaitUntilAsync(() => handler.Count == 1, "delivery");
        Assert.True(handler.Received.TryPeek(out var received));
        Assert.NotEqual(senderThread, received.ThreadId);
    }

    [Fact]
    public async Task OneWayMessages_PreserveOrderPerConnectionAndDirection()
    {
        var network = new InMemoryNetwork();
        var handler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        const int count = 2000;
        for (uint i = 0; i < count; i++)
            await connection.SendAsync(Frame(7, i, (int)(i % 17)), Ct);
        await WaitUntilAsync(() => handler.Count == count, "all messages");

        uint expected = 0;
        foreach ((byte[] payload, _, _) in handler.Received)
            Assert.Equal(expected++, SequenceOf(payload));
    }

    [Fact]
    public async Task Requests_AreDispatchedInOrder_ButNotSerialised()
    {
        var network = new InMemoryNetwork();
        var release = new TaskCompletionSource<ReadOnlyMemory<byte>?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new DelegateHandler((_, request, _, _) =>
            SequenceOf(request.Span) == 0 ? new ValueTask<ReadOnlyMemory<byte>?>(release.Task) : ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray()));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        Task<ReadOnlyMemory<byte>> first = connection.CallAsync(Frame(1, 0), Soon, Ct);
        Task<ReadOnlyMemory<byte>> second = connection.CallAsync(Frame(1, 1), Soon, Ct);

        // The second request completes while the first handler is still pending.
        ReadOnlyMemory<byte> secondReply = await second.WaitAsync(Soon, Ct);
        Assert.Equal(1u, SequenceOf(secondReply.Span));
        Assert.False(first.IsCompleted);
        release.SetResult(new byte[] { 42 });
        Assert.Equal(new byte[] { 42 }, (await first.WaitAsync(Soon, Ct)).ToArray());
        Assert.Equal([0u, 1u], handler.Received.Select(r => SequenceOf(r.Payload)).ToArray());
    }

    [Fact]
    public async Task ConcurrentCalls_WithOutOfOrderReplies_ReachTheirOwnCallers()
    {
        var network = new InMemoryNetwork();
        const int count = 64;
        var releases = new ConcurrentDictionary<uint, TaskCompletionSource<ReadOnlyMemory<byte>?>>();
        var handler = new DelegateHandler((_, request, _, _) =>
        {
            uint sequence = SequenceOf(request.Span);
            var tcs = releases.GetOrAdd(sequence, _ => new TaskCompletionSource<ReadOnlyMemory<byte>?>(TaskCreationOptions.RunContinuationsAsynchronously));
            return new ValueTask<ReadOnlyMemory<byte>?>(tcs.Task);
        });
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        var calls = Enumerable.Range(0, count).Select(i => connection.CallAsync(Frame(5, (uint)i), Soon, Ct)).ToArray();
        await WaitUntilAsync(() => releases.Count == count, "all requests delivered");

        // Reply in reverse order, each with a reply naming its own request.
        for (int i = count - 1; i >= 0; i--)
            releases[(uint)i].SetResult(Frame(6, (uint)(i * 10)));
        ReadOnlyMemory<byte>[] replies = await Task.WhenAll(calls).WaitAsync(Soon, Ct);
        for (int i = 0; i < count; i++)
            Assert.Equal((uint)(i * 10), SequenceOf(replies[i].Span));
    }

    [Fact]
    public async Task Call_TimesOut_WhenNoReplyArrives()
    {
        var time = new ManualTimeProvider();
        var network = new InMemoryNetwork(null, time);
        var handler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)); // never replies
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Frame(1, 1), TimeSpan.FromSeconds(5), Ct);
        await WaitUntilAsync(() => handler.Count == 1, "request delivered");
        time.Advance(TimeSpan.FromSeconds(4));
        await Task.Delay(20, Ct);
        Assert.False(call.IsCompleted);

        time.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<TimeoutException>(() => call.WaitAsync(Soon, Ct));
        Assert.True(connection.IsOpen);
        Assert.Equal(0, time.ScheduledTimers);
    }

    [Fact]
    public async Task Call_WithRealClock_TimesOut()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)));
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 1), TimeSpan.FromMilliseconds(50), Ct).WaitAsync(Soon, Ct));
        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 2), TimeSpan.Zero, Ct));
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = connection.CallAsync(Frame(1, 3), TimeSpan.FromSeconds(-2), Ct); });
    }

    [Fact]
    public async Task LateReply_AfterTimeout_IsIgnored()
    {
        var time = new ManualTimeProvider();
        var network = new InMemoryNetwork(null, time);
        var release = new TaskCompletionSource<ReadOnlyMemory<byte>?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new DelegateHandler((_, request, _, _) =>
            SequenceOf(request.Span) == 1 ? new ValueTask<ReadOnlyMemory<byte>?>(release.Task) : ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray()));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        Task<ReadOnlyMemory<byte>> first = connection.CallAsync(Frame(1, 1), TimeSpan.FromSeconds(1), Ct);
        await WaitUntilAsync(() => handler.Count == 1, "first request delivered");
        time.Advance(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAsync<TimeoutException>(() => first.WaitAsync(Soon, Ct));

        release.SetResult(Frame(1, 999)); // the old reply arrives after the timeout
        ReadOnlyMemory<byte> second = await connection.CallAsync(Frame(1, 2), Soon, Ct);
        Assert.Equal(2u, SequenceOf(second.Span));
    }

    [Fact]
    public async Task Call_Cancellation_CancelsTheWait()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)));
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        using var cts = new CancellationTokenSource();
        Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(Soon, Ct));

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connection.CallAsync(Frame(1, 2), Soon, cancelled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await connection.SendAsync(Frame(1, 3), cancelled.Token));
    }

    [Fact]
    public async Task HandlerException_BecomesAnInternalErrorReply()
    {
        var network = new InMemoryNetwork();
        var handler = new DelegateHandler((_, request, _, _) =>
        {
            if (SequenceOf(request.Span) == 1)
                throw new InvalidOperationException("boom");
            return ValueTask.FromException<ReadOnlyMemory<byte>?>(new IOException("async boom"));
        });
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        ReadOnlyMemory<byte> syncFailure = await connection.CallAsync(Frame(1, 1), Soon, Ct);
        ReadOnlyMemory<byte> asyncFailure = await connection.CallAsync(Frame(1, 2), Soon, Ct);

        var (code, message, _, retryable) = ErrorCodec.Decode(syncFailure.Span);
        Assert.Equal(DfsErrorCode.Internal, code);
        Assert.Contains("boom", message, StringComparison.Ordinal);
        Assert.False(retryable);
        Assert.Equal(DfsErrorCode.Internal, ErrorCodec.Decode(asyncFailure.Span).Code);
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task Connections_AreBidirectional_AndUnstartedEndpointsAnswerUnavailable()
    {
        var network = new InMemoryNetwork();
        var serverConnections = new ConcurrentQueue<IRpcConnection>();
        var serverHandler = new DelegateHandler((connection, request, _, _) =>
        {
            serverConnections.Enqueue(connection);
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray());
        });
        await using IRpcTransport server = await StartAsync(network, "n0", serverHandler);

        // A client that listens too: the server can call it back over the same connection.
        var clientHandler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(new byte[] { 77 }));
        await using IRpcTransport client = await StartAsync(network, "c1", clientHandler);
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        await connection.CallAsync(Frame(1, 1), Soon, Ct);
        Assert.True(serverConnections.TryPeek(out IRpcConnection? inbound));
        Assert.Equal("mem://c1", inbound!.Remote.ToString());
        Assert.Equal(new byte[] { 77 }, (await inbound.CallAsync(Frame(2, 2), Soon, Ct)).ToArray());

        // A connect-only client never started: requests to it are answered with Unavailable, never left hanging.
        await using IRpcTransport passive = network.Factory.Create("c2", "mem://c2");
        IRpcConnection passiveConnection = await passive.ConnectAsync("mem://n0", Ct);
        await passiveConnection.CallAsync(Frame(1, 3), Soon, Ct);
        IRpcConnection inboundFromPassive = serverConnections.Last();
        ReadOnlyMemory<byte> error = await inboundFromPassive.CallAsync(Frame(3, 3), Soon, Ct);
        Assert.Equal(DfsErrorCode.Unavailable, ErrorCodec.Decode(error.Span).Code);
    }

    [Fact]
    public async Task Connect_Fails_ForMissingStoppedOrRefusedEndpoints()
    {
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy);
        IRpcTransport server = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");

        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync("mem://nobody", Ct));
        Assert.Throws<ArgumentException>(() => { _ = client.ConnectAsync("quic://127.0.0.1:1", Ct); });
        Assert.Throws<FormatException>(() => { _ = client.ConnectAsync("not an endpoint", Ct); });

        policy.OnCanConnect((from, to) => !(from == "c" && to == "n0"));
        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync("mem://n0", Ct));
        Assert.Contains(("c", "n0"), policy.ConnectChecks);

        policy.OnCanConnect((_, _) => true);
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        Assert.True(connection.IsOpen);

        await server.DisposeAsync();
        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync("mem://n0", Ct));
        Assert.DoesNotContain("n0", network.Endpoints);
    }

    [Fact]
    public async Task Start_Fails_WhenTheEndpointIsInUse()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport first = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport second = network.Factory.Create("n0b", "mem://n0");
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.StartAsync(DelegateHandler.Echo(), Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => first.StartAsync(DelegateHandler.Echo(), Ct));
        Assert.Throws<ArgumentException>(() => network.Factory.Create("x", "quic://127.0.0.1:0"));
        Assert.Throws<ArgumentException>(() => network.Factory.Create("", "mem://x"));
    }

    [Fact]
    public async Task DisposingAConnection_ClosesBothEnds_AndFailsPendingCalls()
    {
        var network = new InMemoryNetwork();
        var serverConnections = new ConcurrentQueue<IRpcConnection>();
        var handler = new DelegateHandler((connection, _, _, _) =>
        {
            serverConnections.Enqueue(connection);
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null); // never replies
        });
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        var localClosed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Closed += (_, ex) => localClosed.TrySetResult(ex);

        Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);
        await WaitUntilAsync(() => !serverConnections.IsEmpty, "request delivered");
        Assert.True(serverConnections.TryPeek(out IRpcConnection? inbound));
        var remoteClosed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        inbound!.Closed += (_, ex) => remoteClosed.TrySetResult(ex);

        await connection.DisposeAsync();

        Assert.False(connection.IsOpen);
        await Assert.ThrowsAsync<RpcConnectionException>(() => pending.WaitAsync(Soon, Ct));
        Assert.Null(await localClosed.Task.WaitAsync(Soon, Ct));
        Assert.IsType<RpcConnectionException>(await remoteClosed.Task.WaitAsync(Soon, Ct));
        Assert.False(inbound.IsOpen);
        await Assert.ThrowsAsync<RpcConnectionException>(() => connection.CallAsync(Frame(1, 2), Soon, Ct));
        await Assert.ThrowsAsync<RpcConnectionException>(async () => await connection.SendAsync(Frame(1, 3), Ct));

        // A handler subscribed after the close still learns about it.
        var late = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        inbound.Closed += (_, ex) => late.TrySetResult(ex);
        Assert.IsType<RpcConnectionException>(await late.Task.WaitAsync(Soon, Ct));
    }

    [Fact]
    public async Task MessagesSentBeforeClose_AreDeliveredBeforeTheClose()
    {
        var network = new InMemoryNetwork();
        var handler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        for (uint i = 0; i < 100; i++)
            await connection.SendAsync(Frame(1, i), Ct);
        await connection.DisposeAsync();

        await WaitUntilAsync(() => handler.Count == 100, "messages sent before the close");
    }

    [Fact]
    public async Task DisposingATransport_ClosesItsConnections_RemoteSeesClosed()
    {
        var network = new InMemoryNetwork();
        IRpcTransport server = await StartAsync(network, "n0", new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)));
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Closed += (_, ex) => closed.TrySetResult(ex);
        Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);

        await server.DisposeAsync();

        Assert.IsType<RpcConnectionException>(await closed.Task.WaitAsync(Soon, Ct));
        await Assert.ThrowsAsync<RpcConnectionException>(() => pending.WaitAsync(Soon, Ct));
        Assert.False(connection.IsOpen);
        Assert.Equal(0, ((IRpcTransportControl)server).OpenConnectionCount);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => server.ConnectAsync("mem://c", Ct));
    }

    [Fact]
    public async Task ResetConnections_ClosesMatchingConnections_AndAllowsReconnect()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport c1 = network.Factory.Create("c1", "mem://c1");
        await using IRpcTransport c2 = network.Factory.Create("c2", "mem://c2");
        IRpcConnection a = await c1.ConnectAsync("mem://n0", Ct);
        IRpcConnection b = await c2.ConnectAsync("mem://n0", Ct);
        var aClosed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        a.Closed += (_, ex) => aClosed.TrySetResult(ex);
        var control = (IRpcTransportControl)server;
        Assert.Equal("n0", control.LocalName);
        Assert.Equal(2, control.OpenConnectionCount);

        control.ResetConnections("c1");

        Assert.IsType<RpcConnectionException>(await aClosed.Task.WaitAsync(Soon, Ct));
        Assert.True(b.IsOpen);
        Assert.Equal(1, control.OpenConnectionCount);
        IRpcConnection again = await c1.ConnectAsync("mem://n0", Ct);
        Assert.Equal(Frame(1, 1), (await again.CallAsync(Frame(1, 1), Soon, Ct)).ToArray());

        ((IRpcTransportControl)c2).ResetConnections();
        await WaitUntilAsync(() => !b.IsOpen, "local reset closes the connection");
        await WaitUntilAsync(() => control.OpenConnectionCount == 1, "remote end observes the reset");
    }

    [Fact]
    public async Task NetworkResetConnections_ClosesEveryConnectionOfAName_InBothDirections()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport n0 = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport n1 = await StartAsync(network, "n1", DelegateHandler.Echo());
        await using IRpcTransport n2 = await StartAsync(network, "n2", DelegateHandler.Echo());
        IRpcConnection n0ToN1 = await n0.ConnectAsync("mem://n1", Ct);
        IRpcConnection n1ToN2 = await n1.ConnectAsync("mem://n2", Ct);
        IRpcConnection n2ToN0 = await n2.ConnectAsync("mem://n0", Ct);

        network.ResetConnections("n1");

        await WaitUntilAsync(() => !n0ToN1.IsOpen && !n1ToN2.IsOpen, "connections of n1 closed");
        Assert.True(n2ToN0.IsOpen);
        await WaitUntilAsync(() => ((IRpcTransportControl)n1).OpenConnectionCount == 0, "n1 has no connections");
        Assert.Equal(1, ((IRpcTransportControl)n2).OpenConnectionCount);
        IRpcConnection again = await n0.ConnectAsync("mem://n1", Ct);
        Assert.Equal(Frame(1, 1), (await again.CallAsync(Frame(1, 1), Soon, Ct)).ToArray());
    }

    [Fact]
    public async Task LinkPolicy_SeesNamesDirectionsAndPayloads()
    {
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy);
        await using IRpcTransport server = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport client = network.Factory.Create("client:c0", "mem://c0");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        await connection.CallAsync(Frame(40, 1), Soon, Ct);
        await connection.SendAsync(Frame(41, 2), Ct);
        await WaitUntilAsync(() => policy.Decisions.Count == 3, "three decisions");

        var decisions = policy.Decisions.ToArray();
        Assert.Contains(decisions, d => d is { From: "client:c0", To: "n0", Direction: LinkDirection.Request } && TypeOf(d.Payload) == 40);
        Assert.Contains(decisions, d => d is { From: "n0", To: "client:c0", Direction: LinkDirection.Reply } && TypeOf(d.Payload) == 40);
        Assert.Contains(decisions, d => d is { From: "client:c0", To: "n0", Direction: LinkDirection.OneWay } && TypeOf(d.Payload) == 41);
    }

    [Fact]
    public async Task LinkPolicy_DropRequest_TimesOutWithoutExecuting()
    {
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy);
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        policy.OnDecide((_, _, direction, _) => direction == LinkDirection.Request ? LinkDecision.Drop : LinkDecision.Deliver);

        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 1), TimeSpan.FromMilliseconds(100), Ct).WaitAsync(Soon, Ct));
        Assert.Equal(0, handler.Count);
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task LinkPolicy_DropReply_ExecutesOnceButTimesOut()
    {
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy);
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        policy.OnDecide((_, _, direction, _) => direction == LinkDirection.Reply ? LinkDecision.Drop : LinkDecision.Deliver);

        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 1), TimeSpan.FromMilliseconds(100), Ct).WaitAsync(Soon, Ct));
        Assert.Equal(1, handler.Count);
    }

    [Fact]
    public async Task LinkPolicy_BlackHole_KeepsTheConnectionOpenButDeliversNothing()
    {
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy);
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        policy.OnDecide((from, _, _, _) => from == "c" ? LinkDecision.BlackHole : LinkDecision.Deliver);

        await connection.SendAsync(Frame(1, 1), Ct);
        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 2), TimeSpan.FromMilliseconds(100), Ct).WaitAsync(Soon, Ct));
        Assert.Equal(0, handler.Count);
        Assert.True(connection.IsOpen);

        policy.OnDecide((_, _, _, _) => LinkDecision.Deliver); // heal at runtime
        Assert.Equal(Frame(1, 3), (await connection.CallAsync(Frame(1, 3), Soon, Ct)).ToArray());
    }

    [Fact]
    public async Task LinkPolicy_Duplicate_DeliversTwice_AndTheSecondReplyIsIgnored()
    {
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy);
        int calls = 0;
        var handler = new DelegateHandler((_, request, _, _) =>
            ValueTask.FromResult<ReadOnlyMemory<byte>?>(Frame(2, (uint)Interlocked.Increment(ref calls))));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        policy.OnDecide((_, _, direction, _) => direction != LinkDirection.Reply ? LinkDecision.Duplicate : LinkDecision.Deliver);

        ReadOnlyMemory<byte> reply = await connection.CallAsync(Frame(1, 1), Soon, Ct);
        await WaitUntilAsync(() => handler.Count == 2, "duplicated request executed twice");
        Assert.Equal(1u, SequenceOf(reply.Span));

        await connection.SendAsync(Frame(3, 3), Ct);
        await WaitUntilAsync(() => handler.Count == 4, "duplicated one-way delivered twice");
        Assert.Equal(2, handler.Received.Count(r => TypeOf(r.Payload) == 3));
    }

    [Fact]
    public async Task LinkPolicy_Delay_HoldsTheMessage_AndLaterMessagesOvertakeIt()
    {
        var time = new ManualTimeProvider();
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy, time);
        var handler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        policy.OnDecide((_, _, _, payload) => SequenceOf(payload) == 1 ? LinkDecision.Delayed(TimeSpan.FromSeconds(3)) : LinkDecision.Deliver);

        await connection.SendAsync(Frame(1, 1), Ct);
        await connection.SendAsync(Frame(1, 2), Ct);
        await WaitUntilAsync(() => handler.Count == 1, "undelayed message");
        Assert.True(handler.Received.TryPeek(out var first));
        Assert.Equal(2u, SequenceOf(first.Payload));

        time.Advance(TimeSpan.FromSeconds(3));
        await WaitUntilAsync(() => handler.Count == 2, "delayed message");
        Assert.Equal([2u, 1u], handler.Received.Select(r => SequenceOf(r.Payload)).ToArray());
    }

    [Fact]
    public async Task LinkPolicy_DelayedMessages_AreDiscardedWhenTheConnectionCloses()
    {
        var time = new ManualTimeProvider();
        var policy = new ScriptedLinkPolicy();
        var network = new InMemoryNetwork(policy, time);
        var handler = new DelegateHandler((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null));
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        policy.OnDecide((_, _, _, _) => LinkDecision.Delayed(TimeSpan.FromSeconds(1)));

        await connection.SendAsync(Frame(1, 1), Ct);
        await WaitUntilAsync(() => time.ScheduledTimers == 1, "delay scheduled");
        await connection.DisposeAsync();
        await WaitUntilAsync(() => time.ScheduledTimers == 0, "delay cancelled by the close");
        time.Advance(TimeSpan.FromSeconds(2));
        await Task.Delay(50, Ct);
        Assert.Equal(0, handler.Count);
    }

    [Fact]
    public async Task Policy_IsSettableAtRuntime()
    {
        var network = new InMemoryNetwork();
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        Assert.Same(AllowAllLinkPolicy.Instance, network.Policy);

        var drop = new ScriptedLinkPolicy();
        drop.OnDecide((_, _, _, _) => LinkDecision.Drop);
        network.Policy = drop;
        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 1), TimeSpan.FromMilliseconds(50), Ct).WaitAsync(Soon, Ct));

        network.Policy = null!;
        Assert.Same(AllowAllLinkPolicy.Instance, network.Policy);
        Assert.Equal(Frame(1, 2), (await connection.CallAsync(Frame(1, 2), Soon, Ct)).ToArray());
    }

    [Fact]
    public async Task LargePayloads_RoundTripIntact()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        byte[] payload = Pattern(16 << 20, 1);

        ReadOnlyMemory<byte> reply = await connection.CallAsync(payload, Soon, Ct);

        Assert.True(reply.Span.SequenceEqual(payload));
    }

    [Fact]
    public async Task Payloads_AreCopied_SoTheSenderMayReuseItsBuffer()
    {
        var network = new InMemoryNetwork();
        using var gate = new ManualResetEventSlim(false);
        var handler = new DelegateHandler((_, _, _, _) =>
        {
            gate.Wait(Soon);
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);
        });
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        byte[] buffer = Frame(1, 1);
        await connection.SendAsync(buffer, Ct);
        Array.Clear(buffer);
        gate.Set();
        await WaitUntilAsync(() => handler.Count == 1, "delivery");
        Assert.True(handler.Received.TryPeek(out var received));
        Assert.Equal(Frame(1, 1), received.Payload);
    }

    [Fact]
    public async Task HandlerToken_IsCancelledWhenTheConnectionCloses()
    {
        var network = new InMemoryNetwork();
        var observed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new DelegateHandler(async (_, _, _, ct) =>
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                observed.TrySetResult(true);
                throw;
            }

            return null;
        });
        await using IRpcTransport server = await StartAsync(network, "n0", handler);
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);

        Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);
        await WaitUntilAsync(() => handler.Count == 1, "request delivered");
        await connection.DisposeAsync();

        Assert.True(await observed.Task.WaitAsync(Soon, Ct));
        await Assert.ThrowsAsync<RpcConnectionException>(() => call.WaitAsync(Soon, Ct));
    }

    [Fact]
    public async Task Identity_IsSettable()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", DelegateHandler.Echo());
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        Assert.Null(connection.Identity);
        var identity = new RpcPeerIdentity("n0", null, 3, null, IsNode: true, IsBackup: false);
        connection.Identity = identity;
        Assert.Same(identity, connection.Identity);
    }
}
