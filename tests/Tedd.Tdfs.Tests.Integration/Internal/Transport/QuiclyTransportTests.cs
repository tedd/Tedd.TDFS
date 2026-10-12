using System.Collections.Concurrent;
using System.Security.Cryptography;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Integration.Internal.Transport.QuicTestData;

namespace Tedd.Tdfs.Tests.Integration.Internal.Transport;

/// <summary>The Quicly (QUIC over MsQuic) RPC transport over real loopback connections.</summary>
public sealed class QuiclyTransportTests : IClassFixture<QuicCertificateFixture>
{
    private readonly QuicCertificateFixture _fixture;

    public QuiclyTransportTests(QuicCertificateFixture fixture)
    {
        _fixture = fixture;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private QuiclyTransportFactory Factory(ILinkPolicy? policy = null, Func<QuiclyTransportOptions, QuiclyTransportOptions>? configure = null)
    {
        var options = new QuiclyTransportOptions(_fixture.Certificate, [_fixture.Pin], policy)
        {
            Diagnostic = message => TestContext.Current.SendDiagnosticMessage(message),
        };
        return new QuiclyTransportFactory(configure is null ? options : configure(options));
    }

    private static async Task<IRpcTransport> StartAsync(QuiclyTransportFactory factory, string name, IRpcHandler handler)
    {
        IRpcTransport transport = factory.Create(name, AnyLoopback);
        await transport.StartAsync(handler, Ct);
        return transport;
    }

    [Fact]
    public async Task Echo_CallAndOneWay_RoundTrip()
    {
        QuiclyTransportFactory factory = Factory();
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport client = factory.Create("client:c0", AnyLoopback);

        Assert.StartsWith("quic://127.0.0.1:", server.LocalEndpoint, StringComparison.Ordinal);
        Assert.NotEqual(0, RpcAddress.Parse(server.LocalEndpoint).Port);
        Assert.Equal("quic://127.0.0.1:0", client.LocalEndpoint); // never started: the configured endpoint
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.True(connection.IsOpen);
        Assert.Equal(server.LocalEndpoint, connection.Remote.ToString());

        ReadOnlyMemory<byte> reply = await connection.CallAsync(Frame(10, 1, 100), Soon, Ct);
        Assert.Equal(Frame(10, 1, 100), reply.ToArray());

        await connection.SendAsync(Frame(11, 2), Ct);
        await WaitUntilAsync(() => handler.Count == 2, "one-way delivery");
        Assert.Contains(handler.Received, r => !r.ExpectsReply && SequenceOf(r.Payload) == 2);

        ReadOnlyMemory<byte> empty = await connection.CallAsync(ReadOnlyMemory<byte>.Empty, Soon, Ct);
        Assert.True(empty.IsEmpty);

        // Compressible payloads (LZ4 on the control channel) survive intact.
        byte[] compressible = new byte[50_000];
        Assert.Equal(compressible, (await connection.CallAsync(compressible, Soon, Ct)).ToArray());
    }

    [Fact]
    public async Task InboundConnection_IsBidirectional_AndCarriesTheClientsIdentity()
    {
        QuiclyTransportFactory factory = Factory();
        var serverHandler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(factory, "n0", serverHandler);
        var clientHandler = new DelegateHandler((_, request, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(Frame(99, SequenceOf(request.Span))));
        await using IRpcTransport client = await StartAsync(factory, "n1", clientHandler);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        await connection.CallAsync(Frame(1, 1), Soon, Ct);

        Assert.True(serverHandler.Received.TryPeek(out var received));
        IRpcConnection inbound = received.Connection;
        Assert.StartsWith("quic://127.0.0.1:", inbound.Remote.ToString(), StringComparison.Ordinal);
        ReadOnlyMemory<byte> reply = await inbound.CallAsync(Frame(2, 7), Soon, Ct);
        Assert.Equal(Frame(99, 7), reply.ToArray());

        // A connect-only transport answers server-initiated requests with Unavailable instead of leaving them hanging.
        await using IRpcTransport passive = factory.Create("c2", AnyLoopback);
        IRpcConnection passiveConnection = await passive.ConnectAsync(server.LocalEndpoint, Ct);
        await passiveConnection.CallAsync(Frame(1, 3), Soon, Ct);
        IRpcConnection inboundFromPassive = serverHandler.Received.Last().Connection;
        ReadOnlyMemory<byte> error = await inboundFromPassive.CallAsync(Frame(3, 3), Soon, Ct);
        Assert.Equal(DfsErrorCode.Unavailable, ErrorCodec.Decode(error.Span).Code);
    }

    [Fact]
    public async Task HandlerException_BecomesAnInternalErrorReply()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport server = await StartAsync(factory, "n0", new DelegateHandler((_, _, _, _) => throw new InvalidOperationException("kaboom")));
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);

        ReadOnlyMemory<byte> reply = await connection.CallAsync(Frame(1, 1), Soon, Ct);

        var (code, message, _, _) = ErrorCodec.Decode(reply.Span);
        Assert.Equal(DfsErrorCode.Internal, code);
        Assert.Contains("kaboom", message, StringComparison.Ordinal);
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task TenTransports_FullyMeshed_ExchangeCallsOverEveryLink()
    {
        QuiclyTransportFactory factory = Factory();
        const int count = 10;
        var transports = new IRpcTransport[count];
        try
        {
            for (int i = 0; i < count; i++)
            {
                int self = i;
                transports[i] = await StartAsync(factory, "n" + i, new DelegateHandler((_, request, _, _) =>
                    ValueTask.FromResult<ReadOnlyMemory<byte>?>(Frame((ushort)self, SequenceOf(request.Span)))));
            }

            var connections = new IRpcConnection[count, count];
            await Parallel.ForAsync(0, count * count, Ct, async (k, ct) =>
            {
                int from = k / count, to = k % count;
                if (from != to)
                    connections[from, to] = await transports[from].ConnectAsync(transports[to].LocalEndpoint, ct);
            });

            await Parallel.ForAsync(0, count * count, Ct, async (k, ct) =>
            {
                int from = k / count, to = k % count;
                if (from == to)
                    return;
                for (uint round = 0; round < 5; round++)
                {
                    uint sequence = (uint)(k * 100) + round;
                    ReadOnlyMemory<byte> reply = await connections[from, to].CallAsync(Frame(1, sequence), Soon, ct);
                    Assert.Equal((ushort)to, TypeOf(reply.Span));
                    Assert.Equal(sequence, SequenceOf(reply.Span));
                }
            });

            for (int i = 0; i < count; i++)
                Assert.Equal(2 * (count - 1), ((IRpcTransportControl)transports[i]).OpenConnectionCount);
        }
        finally
        {
            await Task.WhenAll(transports.Where(t => t is not null).Select(t => t.DisposeAsync().AsTask()));
        }
    }

    [Fact]
    public async Task EightMiBPayloads_AreFragmentedAndReassembled_InBothDirections()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport server = await StartAsync(factory, "n0", new DelegateHandler((_, request, _, _) =>
        {
            byte[] reply = request.ToArray();
            Array.Reverse(reply);
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(reply);
        }));
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        byte[] payload = Pattern(8 << 20, 42);
        byte[] expected = payload.ToArray();
        Array.Reverse(expected);

        // Small calls keep flowing on the control channel while the bulk transfer runs on the data channel.
        Task<ReadOnlyMemory<byte>> bulk = connection.CallAsync(payload, Soon, Ct);
        for (uint i = 0; i < 20; i++)
            Assert.Equal(Frame(1, i), Reverse((await connection.CallAsync(Frame(1, i), Soon, Ct)).ToArray()));
        ReadOnlyMemory<byte> reply = await bulk;

        Assert.Equal(SHA256.HashData(expected), SHA256.HashData(reply.Span));

        // Several large messages at once (fragments of different messages interleave on the unordered channel).
        Task<ReadOnlyMemory<byte>>[] concurrent = Enumerable.Range(0, 4)
            .Select(i => connection.CallAsync(Pattern((1 << 20) + i * 4321, i), Soon, Ct)).ToArray();
        ReadOnlyMemory<byte>[] replies = await Task.WhenAll(concurrent);
        for (int i = 0; i < replies.Length; i++)
            Assert.Equal(Reverse(Pattern((1 << 20) + i * 4321, i)), replies[i].ToArray());
    }

    [Fact]
    public async Task ConcurrentCalls_FromManyThreads_AreCorrelatedCorrectly()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport server = await StartAsync(factory, "n0", new DelegateHandler(async (_, request, _, _) =>
        {
            uint sequence = SequenceOf(request.Span);
            if (sequence % 7 == 0)
                await Task.Delay(5); // replies complete out of order
            return Frame(2, sequence * 3, (int)(sequence % 300));
        }));
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);

        const int threads = 16;
        const int callsPerThread = 64;
        var failures = new ConcurrentQueue<string>();
        var workers = Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
        {
            for (int i = 0; i < callsPerThread; i++)
            {
                uint sequence = (uint)(t * callsPerThread + i);
                ReadOnlyMemory<byte> reply = connection.CallAsync(Frame(1, sequence, (int)(sequence % 500)), Soon, Ct).GetAwaiter().GetResult();
                if (!reply.Span.SequenceEqual(Frame(2, sequence * 3, (int)(sequence % 300))))
                    failures.Enqueue($"call {sequence} got the wrong reply");
            }
        }, Ct, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
        await Task.WhenAll(workers);

        Assert.Empty(failures);
    }

    [Fact]
    public async Task SmallOneWayMessages_PreserveOrder()
    {
        QuiclyTransportFactory factory = Factory();
        var handler = DelegateHandler.Silent();
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);

        const int count = 2000;
        for (uint i = 0; i < count; i++)
            await connection.SendAsync(Frame(5, i, (int)(i % 1000)), Ct);
        await WaitUntilAsync(() => handler.Count == count, "all one-way messages");

        uint expected = 0;
        foreach (var received in handler.Received)
            Assert.Equal(expected++, SequenceOf(received.Payload));
    }

    [Fact]
    public async Task Call_TimesOut_AndALateReplyIsIgnored()
    {
        QuiclyTransportFactory factory = Factory();
        var release = new TaskCompletionSource<ReadOnlyMemory<byte>?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using IRpcTransport server = await StartAsync(factory, "n0", new DelegateHandler((_, request, _, _) =>
            SequenceOf(request.Span) == 1 ? new ValueTask<ReadOnlyMemory<byte>?>(release.Task) : ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray())));
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);

        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 1), TimeSpan.FromMilliseconds(200), Ct));
        release.SetResult(Frame(1, 999));
        Assert.Equal(Frame(1, 2), (await connection.CallAsync(Frame(1, 2), Soon, Ct)).ToArray());
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task ServerStop_ClientSeesClosed_AndPendingCallsFail()
    {
        QuiclyTransportFactory factory = Factory();
        IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Silent());
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        TaskCompletionSource<Exception?> closed = WatchClosed(connection);
        Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);
        await Task.Delay(50, Ct);
        string endpoint = server.LocalEndpoint;

        await server.DisposeAsync();

        Exception? error = await closed.Task.WaitAsync(Soon, Ct);
        Assert.IsType<RpcConnectionException>(error);
        await Assert.ThrowsAsync<RpcConnectionException>(() => pending.WaitAsync(Soon, Ct));
        Assert.False(connection.IsOpen);
        await Assert.ThrowsAsync<RpcConnectionException>(() => connection.CallAsync(Frame(1, 2), Soon, Ct));
        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync(endpoint, Ct));
        Assert.Equal(0, ((IRpcTransportControl)client).OpenConnectionCount);
    }

    [Fact]
    public async Task ClientDisposesConnection_ServerSeesClosed()
    {
        QuiclyTransportFactory factory = Factory();
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        await connection.CallAsync(Frame(1, 1), Soon, Ct);
        IRpcConnection inbound = handler.Received.Single().Connection;
        TaskCompletionSource<Exception?> remoteClosed = WatchClosed(inbound);
        TaskCompletionSource<Exception?> localClosed = WatchClosed(connection);

        await connection.DisposeAsync();

        Assert.Null(await localClosed.Task.WaitAsync(Soon, Ct));
        Assert.IsType<RpcConnectionException>(await remoteClosed.Task.WaitAsync(Soon, Ct));
        Assert.False(inbound.IsOpen);
        await WaitUntilAsync(() => ((IRpcTransportControl)server).OpenConnectionCount == 0, "server forgets the connection");
    }

    [Fact]
    public async Task WrongSpkiPin_ConnectFails()
    {
        QuiclyTransportFactory serverFactory = Factory();
        await using IRpcTransport server = await StartAsync(serverFactory, "n0", DelegateHandler.Echo());
        var wrongPin = new byte[32];
        RandomNumberGenerator.Fill(wrongPin);
        var clientFactory = new QuiclyTransportFactory(new QuiclyTransportOptions(null!, [wrongPin])
        {
            ConnectTimeout = TimeSpan.FromSeconds(10),
        });
        await using IRpcTransport client = clientFactory.Create("c", AnyLoopback);

        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync(server.LocalEndpoint, Ct));
        Assert.Equal(0, ((IRpcTransportControl)client).OpenConnectionCount);
    }

    [Fact]
    public async Task CertificateCallback_AndDevelopmentMode_CanAcceptAnUnpinnedServer()
    {
        QuiclyTransportFactory serverFactory = Factory();
        await using IRpcTransport server = await StartAsync(serverFactory, "n0", DelegateHandler.Echo());

        int seen = 0;
        var callbackFactory = new QuiclyTransportFactory(new QuiclyTransportOptions(null!, [])
        {
            ValidateServerCertificate = certificate =>
            {
                Interlocked.Increment(ref seen);
                return certificate.Thumbprint == _fixture.Certificate.Thumbprint;
            },
        });
        await using IRpcTransport callbackClient = callbackFactory.Create("c1", AnyLoopback);
        IRpcConnection accepted = await callbackClient.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.Equal(Frame(1, 1), (await accepted.CallAsync(Frame(1, 1), Soon, Ct)).ToArray());
        Assert.True(seen > 0);

        var rejectingFactory = new QuiclyTransportFactory(new QuiclyTransportOptions(null!, []) { ValidateServerCertificate = _ => false });
        await using IRpcTransport rejectingClient = rejectingFactory.Create("c2", AnyLoopback);
        await Assert.ThrowsAsync<RpcConnectionException>(() => rejectingClient.ConnectAsync(server.LocalEndpoint, Ct));

        var developmentFactory = new QuiclyTransportFactory(new QuiclyTransportOptions(null!, []) { AllowUntrustedForDevelopment = true });
        await using IRpcTransport developmentClient = developmentFactory.Create("c3", AnyLoopback);
        IRpcConnection development = await developmentClient.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.True(development.IsOpen);
    }

    [Fact]
    public async Task ConnectToAClosedPort_Fails()
    {
        QuiclyTransportFactory factory = Factory(configure: o => o with { ConnectTimeout = TimeSpan.FromSeconds(5) });
        IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Echo());
        string endpoint = server.LocalEndpoint;
        await server.DisposeAsync();
        await using IRpcTransport client = factory.Create("c", AnyLoopback);

        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync(endpoint, Ct));
    }

    [Fact]
    public async Task InvalidEndpoints_AndOptions_AreRejected()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        await Assert.ThrowsAsync<ArgumentException>(() => client.ConnectAsync("mem://n0", Ct));
        await Assert.ThrowsAsync<FormatException>(() => client.ConnectAsync("quic://nohost", Ct));
        Assert.Throws<ArgumentException>(() => factory.Create("x", "mem://x"));
        Assert.Throws<ArgumentException>(() => factory.Create("", AnyLoopback));
        Assert.Throws<ArgumentException>(() => new QuiclyTransportFactory(new QuiclyTransportOptions(_fixture.Certificate, [new byte[5]])));

        var connectOnly = new QuiclyTransportFactory(new QuiclyTransportOptions(null!, [_fixture.Pin]));
        await using IRpcTransport noCertificate = connectOnly.Create("x", AnyLoopback);
        await Assert.ThrowsAsync<InvalidOperationException>(() => noCertificate.StartAsync(DelegateHandler.Echo(), Ct));

        await using IRpcTransport started = await StartAsync(factory, "n0", DelegateHandler.Echo());
        await Assert.ThrowsAsync<InvalidOperationException>(() => started.StartAsync(DelegateHandler.Echo(), Ct));
        IRpcConnection connection = await client.ConnectAsync(started.LocalEndpoint, Ct);
        var tooLarge = new QuiclyTransportFactory(new QuiclyTransportOptions(null!, [_fixture.Pin]) { MaxMessageBytes = 1000 });
        await using IRpcTransport limited = tooLarge.Create("l", AnyLoopback);
        IRpcConnection limitedConnection = await limited.ConnectAsync(started.LocalEndpoint, Ct);
        Assert.Throws<ArgumentException>(() => { _ = limitedConnection.CallAsync(new byte[1001], Soon, Ct); });
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task StartingOnAPortInUse_Fails()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport first = await StartAsync(factory, "n0", DelegateHandler.Echo());
        await using IRpcTransport second = factory.Create("n1", first.LocalEndpoint);
        await Assert.ThrowsAsync<RpcConnectionException>(() => second.StartAsync(DelegateHandler.Echo(), Ct));
    }

    [Fact]
    public async Task LinkPolicy_CanConnect_RefusesTheConnection()
    {
        var policy = new ScriptedLinkPolicy();
        QuiclyTransportFactory factory = Factory(policy);
        await using IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Echo());
        await using IRpcTransport client = factory.Create("client:c0", AnyLoopback);
        policy.OnCanConnect((from, to) => !(from == "client:c0" && to == "n0"));

        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync(server.LocalEndpoint, Ct));
        Assert.Contains(("client:c0", "n0"), policy.ConnectChecks);

        // Refusals are not rate-limited per address: other localhost peers still connect, and so does this one once healed.
        for (int i = 0; i < 12; i++)
            await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync(server.LocalEndpoint, Ct));
        policy.OnCanConnect((_, _) => true);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.Equal(Frame(1, 1), (await connection.CallAsync(Frame(1, 1), Soon, Ct)).ToArray());
    }

    [Fact]
    public async Task LinkPolicy_CanConnect_IsAlsoHonouredByTheConnectingSide()
    {
        // The server allows everything; only the client's own policy refuses the server it reached (known from the preamble).
        await using IRpcTransport server = await StartAsync(Factory(), "n0", DelegateHandler.Echo());
        var clientPolicy = new ScriptedLinkPolicy();
        clientPolicy.OnCanConnect((from, to) => !(from == "c" && to == "n0"));
        await using IRpcTransport client = Factory(clientPolicy).Create("c", AnyLoopback);

        await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync(server.LocalEndpoint, Ct));
        Assert.Contains(("c", "n0"), clientPolicy.ConnectChecks);
        Assert.Equal(0, ((IRpcTransportControl)client).OpenConnectionCount);
        await WaitUntilAsync(() => ((IRpcTransportControl)server).OpenConnectionCount == 0, "the refused connection is closed on the server too");
    }

    [Fact]
    public async Task LinkPolicy_SeesNamesDirectionsAndFullPayloads()
    {
        var policy = new ScriptedLinkPolicy();
        QuiclyTransportFactory factory = Factory(policy);
        await using IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Echo());
        await using IRpcTransport client = factory.Create("client:c0", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);

        await connection.CallAsync(Frame(40, 1), Soon, Ct);
        await connection.SendAsync(Frame(41, 2), Ct);
        byte[] large = Pattern(200_000, 5);
        large[0] = 42;
        large[1] = 0;
        await connection.CallAsync(large, Soon, Ct);
        await WaitUntilAsync(() => policy.Decisions.Count == 5, "five decisions");

        var decisions = policy.Decisions.ToArray();
        Assert.Contains(decisions, d => d.From == "client:c0" && d.To == "n0" && d.Direction == LinkDirection.Request && TypeOf(d.Payload) == 40);
        Assert.Contains(decisions, d => d.From == "n0" && d.To == "client:c0" && d.Direction == LinkDirection.Reply && TypeOf(d.Payload) == 40);
        Assert.Contains(decisions, d => d.From == "client:c0" && d.To == "n0" && d.Direction == LinkDirection.OneWay && TypeOf(d.Payload) == 41);
        Assert.Contains(decisions, d => d.Direction == LinkDirection.Request && d.Payload.AsSpan().SequenceEqual(large));
    }

    [Fact]
    public async Task LinkPolicy_DropDelayDuplicateAndBlackHole_AreHonoured()
    {
        var policy = new ScriptedLinkPolicy();
        QuiclyTransportFactory factory = Factory(policy);
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);

        // Dropped request: never executed, the caller times out.
        policy.OnDecide((_, _, direction, payload) => direction == LinkDirection.Request && TypeOf(payload) == 1 ? LinkDecision.Drop : LinkDecision.Deliver);
        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(1, 1), TimeSpan.FromMilliseconds(200), Ct));
        Assert.Equal(0, handler.Count);

        // Dropped reply: executed once, the caller times out.
        policy.OnDecide((_, _, direction, _) => direction == LinkDirection.Reply ? LinkDecision.Drop : LinkDecision.Deliver);
        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(2, 2), TimeSpan.FromMilliseconds(200), Ct));
        Assert.Equal(1, handler.Count);

        // Duplicated one-way message: delivered twice.
        policy.OnDecide((_, _, direction, _) => direction == LinkDirection.OneWay ? LinkDecision.Duplicate : LinkDecision.Deliver);
        await connection.SendAsync(Frame(3, 3), Ct);
        await WaitUntilAsync(() => handler.Received.Count(r => TypeOf(r.Payload) == 3) == 2, "duplicate delivery");

        // Delayed message: overtaken by a later one.
        policy.OnDecide((_, _, _, payload) => TypeOf(payload) == 4 ? LinkDecision.Delayed(TimeSpan.FromMilliseconds(300)) : LinkDecision.Deliver);
        await connection.SendAsync(Frame(4, 4), Ct);
        await connection.SendAsync(Frame(5, 5), Ct);
        await WaitUntilAsync(() => handler.Received.Any(r => TypeOf(r.Payload) == 4), "delayed delivery");
        byte[][] order = handler.Received.Select(r => r.Payload).Where(p => TypeOf(p) is 4 or 5).ToArray();
        Assert.Equal([5, 4], order.Select(p => (int)TypeOf(p)).ToArray());

        // Black hole: the connection stays open but nothing arrives; healing restores delivery.
        policy.OnDecide((from, _, _, _) => from == "c" ? LinkDecision.BlackHole : LinkDecision.Deliver);
        await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Frame(6, 6), TimeSpan.FromMilliseconds(200), Ct));
        Assert.True(connection.IsOpen);
        policy.OnDecide((_, _, _, _) => LinkDecision.Deliver);
        Assert.Equal(Frame(7, 7), (await connection.CallAsync(Frame(7, 7), Soon, Ct)).ToArray());
    }

    [Fact]
    public async Task ResetConnections_ClosesBothEnds_AndPeersCanReconnect()
    {
        QuiclyTransportFactory factory = Factory();
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport c1 = factory.Create("c1", AnyLoopback);
        await using IRpcTransport c2 = factory.Create("c2", AnyLoopback);
        IRpcConnection a = await c1.ConnectAsync(server.LocalEndpoint, Ct);
        IRpcConnection b = await c2.ConnectAsync(server.LocalEndpoint, Ct);
        TaskCompletionSource<Exception?> aClosed = WatchClosed(a);
        var control = (IRpcTransportControl)server;
        await WaitUntilAsync(() => control.OpenConnectionCount == 2, "both inbound connections");

        control.ResetConnections("c1");

        Assert.IsType<RpcConnectionException>(await aClosed.Task.WaitAsync(Soon, Ct));
        Assert.True(b.IsOpen);
        Assert.Equal(Frame(1, 1), (await b.CallAsync(Frame(1, 1), Soon, Ct)).ToArray());
        IRpcConnection again = await c1.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.Equal(Frame(1, 2), (await again.CallAsync(Frame(1, 2), Soon, Ct)).ToArray());

        // Resetting from the client side: the server observes it too.
        IRpcConnection inboundFromC2 = handler.Received.First(r => SequenceOf(r.Payload) == 1).Connection;
        TaskCompletionSource<Exception?> inboundClosed = WatchClosed(inboundFromC2);
        ((IRpcTransportControl)c2).ResetConnections();
        Assert.IsType<RpcConnectionException>(await inboundClosed.Task.WaitAsync(Soon, Ct));
        Assert.False(b.IsOpen);
    }

    [Fact]
    public async Task DisposingAClient_WhileCallsArePending_FailsThemPromptly()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Silent());
        IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        Task<ReadOnlyMemory<byte>>[] pending = Enumerable.Range(0, 10)
            .Select(i => connection.CallAsync(Frame(1, (uint)i), Timeout.InfiniteTimeSpan, Ct)).ToArray();
        ValueTask largeSend = connection.SendAsync(Pattern(4 << 20, 1), Ct);

        await client.DisposeAsync();

        foreach (Task<ReadOnlyMemory<byte>> call in pending)
            await Assert.ThrowsAsync<RpcConnectionException>(() => call.WaitAsync(Soon, Ct));
        try
        {
            await largeSend.AsTask().WaitAsync(Soon, Ct);
        }
        catch (RpcConnectionException)
        {
            // Either fully handed to QUIC before the close, or failed by it; never left hanging.
        }

        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.ConnectAsync(server.LocalEndpoint, Ct));
        await client.DisposeAsync(); // idempotent
    }

    [Fact]
    public async Task Mesh_ConcurrentLargeAndSmallCalls_AllComplete()
    {
        QuiclyTransportFactory factory = Factory();
        const int count = 4;
        var transports = new IRpcTransport[count];
        try
        {
            for (int i = 0; i < count; i++)
                transports[i] = await StartAsync(factory, "n" + i, DelegateHandler.Echo());
            long bytes = 0;
            await Parallel.ForAsync(0, count * count, Ct, async (k, ct) =>
            {
                int from = k / count, to = k % count;
                if (from == to)
                    return;
                IRpcConnection connection = await transports[from].ConnectAsync(transports[to].LocalEndpoint, ct);
                var calls = new List<Task>();
                for (int m = 0; m < 10; m++)
                {
                    byte[] payload = Pattern((1 << 20) + m, k * 100 + m);
                    calls.Add(Task.Run(async () =>
                    {
                        ReadOnlyMemory<byte> reply = await connection.CallAsync(payload, Soon, ct);
                        Assert.True(reply.Span.SequenceEqual(payload));
                        Interlocked.Add(ref bytes, payload.Length * 2L);
                    }, ct));
                    calls.Add(Task.Run(async () =>
                    {
                        for (int s = 0; s < 10; s++)
                            Assert.Equal(Frame(1, (uint)s), (await connection.CallAsync(Frame(1, (uint)s), Soon, ct)).ToArray());
                    }, ct));
                }

                await Task.WhenAll(calls);
            });

            Assert.Equal(count * (count - 1) * 10 * ((1L << 20) * 2) + count * (count - 1) * 2L * 45, bytes);
        }
        finally
        {
            await Task.WhenAll(transports.Where(t => t is not null).Select(t => t.DisposeAsync().AsTask()));
        }
    }

    [Fact]
    public async Task Dispose_ReleasesEveryNativeResource()
    {
        QuiclyTransportFactory factory = Factory();
        IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Echo());
        IRpcTransport client = await StartAsync(factory, "n1", DelegateHandler.Echo());
        IRpcConnection outbound = await client.ConnectAsync(server.LocalEndpoint, Ct);
        IRpcConnection backwards = await server.ConnectAsync(client.LocalEndpoint, Ct);
        await outbound.CallAsync(Pattern(3 << 20, 1), Soon, Ct);
        await backwards.CallAsync(Frame(1, 1), Soon, Ct);
        await outbound.DisposeAsync(); // one connection closed before the transports

        await client.DisposeAsync();
        await server.DisposeAsync();

        var serverImpl = Assert.IsType<Tedd.Tdfs.Transport.Internal.QuiclyRpcTransport>(server);
        var clientImpl = Assert.IsType<Tedd.Tdfs.Transport.Internal.QuiclyRpcTransport>(client);
        await serverImpl.NativeResourcesReleased.WaitAsync(Soon, Ct);
        await clientImpl.NativeResourcesReleased.WaitAsync(Soon, Ct);
        await WaitUntilAsync(() => serverImpl.ClientPoolReleased && clientImpl.ClientPoolReleased, "client buffer pools released");
        Assert.Equal(0, ((IRpcTransportControl)server).OpenConnectionCount);
        Assert.Equal(0, ((IRpcTransportControl)client).OpenConnectionCount);
    }

    private static byte[] Reverse(byte[] data)
    {
        byte[] copy = data.ToArray();
        Array.Reverse(copy);
        return copy;
    }
}
