using System.Diagnostics;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Integration.Internal.Transport.QuicTestData;

namespace Tedd.Tdfs.Tests.Integration.Internal.Transport;

/// <summary>Adversarial (review) cases for the Quicly transport over real loopback QUIC.</summary>
public sealed class QuiclyTransportAdversarialTests : IClassFixture<QuicCertificateFixture>
{
    private readonly QuicCertificateFixture _fixture;

    public QuiclyTransportAdversarialTests(QuicCertificateFixture fixture)
    {
        _fixture = fixture;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private QuiclyTransportFactory Factory(Func<QuiclyTransportOptions, QuiclyTransportOptions>? configure = null)
    {
        var options = new QuiclyTransportOptions(_fixture.Certificate, [_fixture.Pin])
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

    /// <summary>
    /// A reply (and one-way messages) that reached the client before the server closed the connection must still be
    /// delivered: the close is ordered behind them, as with the in-memory transport. The client's inbound queue is stalled
    /// by a blocking handler so the close is observed while those messages are still queued.
    /// </summary>
    [Fact]
    public async Task MessagesReceivedBeforeARemoteClose_AreStillDelivered()
    {
        QuiclyTransportFactory factory = Factory();
        var serverHandler = new DelegateHandler(async (connection, request, _, _) =>
        {
            // Stall the client's inbound queue, then reply behind that message on the same ordered channel.
            await connection.SendAsync(Frame(9, 0), CancellationToken.None);
            return Frame(2, SequenceOf(request.Span));
        });
        await using IRpcTransport server = await StartAsync(factory, "n0", serverHandler);
        using var gate = new ManualResetEventSlim(false);
        var stalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clientHandler = new DelegateHandler((_, request, _, _) =>
        {
            if (TypeOf(request.Span) == 9)
            {
                stalled.TrySetResult();
                gate.Wait(Soon);
            }

            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);
        });
        await using IRpcTransport client = await StartAsync(factory, "c", clientHandler);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        TaskCompletionSource<Exception?> closed = WatchClosed(connection);

        Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Frame(1, 7), Soon, Ct);
        await stalled.Task.WaitAsync(Soon, Ct);
        IRpcConnection inbound = serverHandler.Received.Single().Connection;
        for (uint i = 0; i < 5; i++)
            await inbound.SendAsync(Frame(10, i), Ct);
        await Task.Delay(300, Ct);       // the reply and the one-way messages reach the client's (stalled) queue
        await inbound.DisposeAsync();     // graceful close from the server
        await Task.Delay(500, Ct);       // the close reaches the client while its queue is still stalled
        gate.Set();

        Assert.Equal(Frame(2, 7), (await call.WaitAsync(Soon, Ct)).ToArray());
        await WaitUntilAsync(() => clientHandler.Received.Count(r => TypeOf(r.Payload) == 10) == 5, "one-way messages received before the close");
        Assert.IsType<RpcConnectionException>(await closed.Task.WaitAsync(Soon, Ct));
        Assert.False(connection.IsOpen);
        await WaitUntilAsync(() => ((IRpcTransportControl)client).OpenConnectionCount == 0, "the client forgets the connection");
    }

    /// <summary>
    /// A message whose <c>SendAsync</c> completed (handed to QUIC) is delivered even when the sender closes gracefully right
    /// afterwards, by disposing the connection or the whole transport. Fragmented messages are the hard case: up to the
    /// Quicly send budget is still queued when the close starts, and Quicly's close travels on its own stream.
    /// </summary>
    [Fact]
    public async Task AwaitedSends_AreDeliveredBeforeAGracefulClose()
    {
        QuiclyTransportFactory factory = Factory();
        var serverHandler = DelegateHandler.Silent();
        await using IRpcTransport server = await StartAsync(factory, "n0", serverHandler);
        var clientHandler = DelegateHandler.Silent();
        await using IRpcTransport client = await StartAsync(factory, "c", clientHandler);
        const int count = 40;

        // The client disposes its connection.
        for (int round = 0; round < 3; round++)
        {
            IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
            int before = serverHandler.Count;
            for (uint i = 0; i < count; i++)
                await connection.SendAsync(Frame((ushort)(10 + round), i, 70_000), Ct);
            await connection.DisposeAsync();
            await WaitUntilAsync(() => serverHandler.Count - before == count, $"round {round}: {count} messages, got {serverHandler.Count - before}");
        }

        Assert.All(serverHandler.Received, r => Assert.Equal(Frame(TypeOf(r.Payload), SequenceOf(r.Payload), 70_000), r.Payload));

        // The server disposes its whole transport right after sending over an inbound connection.
        IRpcConnection toServer = await client.ConnectAsync(server.LocalEndpoint, Ct);
        TaskCompletionSource<Exception?> closed = WatchClosed(toServer);
        await toServer.SendAsync(Frame(1, 0), Ct);
        await WaitUntilAsync(() => serverHandler.Received.Any(r => TypeOf(r.Payload) == 1), "the inbound connection");
        IRpcConnection inbound = serverHandler.Received.Single(r => TypeOf(r.Payload) == 1).Connection;
        for (uint i = 0; i < count; i++)
            await inbound.SendAsync(Frame(20, i, 70_000), Ct);
        await server.DisposeAsync();

        await WaitUntilAsync(() => clientHandler.Received.Count(r => TypeOf(r.Payload) == 20) == count,
            $"{count} messages from the disposed server, got {clientHandler.Received.Count(r => TypeOf(r.Payload) == 20)}");
        Assert.IsType<RpcConnectionException>(await closed.Task.WaitAsync(Soon, Ct));
    }

    /// <summary>
    /// The pump thread serves every connection of a transport, so handler code must never run on it. Closing a connection
    /// cancels its handlers' token; a handler that resumes on that cancellation and works synchronously must not stall
    /// the other connections of the transport.
    /// </summary>
    [Fact]
    public async Task HandlerCancellationCode_NeverStallsTheTransport()
    {
        QuiclyTransportFactory factory = Factory();
        var cleanup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new DelegateHandler(async (_, request, _, ct) =>
        {
            if (TypeOf(request.Span) != 1)
                return request.ToArray();
            var cancelled = new TaskCompletionSource();
            using (ct.Register(() => cancelled.TrySetCanceled(ct))) // the registration runs on the cancelling thread
            {
                try
                {
                    await cancelled.Task;
                }
                catch (OperationCanceledException)
                {
                    cleanup.TrySetResult();
                    Thread.Sleep(3000); // slow synchronous cleanup after cancellation
                }
            }

            return null;
        });
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport c1 = factory.Create("c1", AnyLoopback);
        await using IRpcTransport c2 = factory.Create("c2", AnyLoopback);
        IRpcConnection doomed = await c1.ConnectAsync(server.LocalEndpoint, Ct);
        IRpcConnection other = await c2.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.Equal(Frame(2, 0), (await other.CallAsync(Frame(2, 0), Soon, Ct)).ToArray());
        Task<ReadOnlyMemory<byte>> slow = doomed.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);
        await WaitUntilAsync(() => handler.Count == 2, "slow request delivered");

        await doomed.DisposeAsync(); // the server learns about the close on its pump thread
        await cleanup.Task.WaitAsync(Soon, Ct);

        var watch = Stopwatch.StartNew();
        Assert.Equal(Frame(2, 1), (await other.CallAsync(Frame(2, 1), Soon, Ct)).ToArray());
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1.5), $"A call on another connection waited {watch.Elapsed} for handler code running on the pump.");
        await Assert.ThrowsAsync<RpcConnectionException>(() => slow.WaitAsync(Soon, Ct));
    }

    /// <summary>
    /// A reset in the middle of a fragmented transfer never delivers a partial message (NET-004/NET-007 at the transport
    /// level): the receiver sees either the complete payload or nothing, both ends close, and a new connection works.
    /// </summary>
    [Fact]
    public async Task ResetDuringAFragmentedTransfer_NeverDeliversAPartialMessage()
    {
        QuiclyTransportFactory factory = Factory();
        var handler = DelegateHandler.Silent();
        await using IRpcTransport server = await StartAsync(factory, "n0", handler);
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        byte[] payload = Pattern(16 << 20, 3);

        for (int round = 0; round < 3; round++)
        {
            IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
            TaskCompletionSource<Exception?> closed = WatchClosed(connection);
            ValueTask send = connection.SendAsync(payload, Ct);
            await Task.Delay(5 + round * 10, Ct);
            ((IRpcTransportControl)server).ResetConnections("c");

            Assert.IsType<RpcConnectionException>(await closed.Task.WaitAsync(Soon, Ct));
            try
            {
                await send.AsTask().WaitAsync(Soon, Ct);
            }
            catch (RpcConnectionException)
            {
                // Failed by the reset before it was handed over completely.
            }
        }

        IRpcConnection healthy = await client.ConnectAsync(server.LocalEndpoint, Ct);
        await healthy.SendAsync(Frame(1, 1), Ct);
        await WaitUntilAsync(() => handler.Received.Any(r => r.Payload.Length == 6), "a message on a new connection");
        Assert.All(handler.Received, r => Assert.True(r.Payload.Length == 6 || r.Payload.AsSpan().SequenceEqual(payload), $"partial message of {r.Payload.Length} bytes"));
    }

    /// <summary>
    /// A peer that sends more than this end accepts (a sender configured with a larger <c>MaxMessageBytes</c>) is treated
    /// as a protocol violation: the receiver closes the connection, and the sender's pending call fails instead of hanging.
    /// Applies to fragmented messages (rejected on their first fragment) and to oversized single-frame messages.
    /// </summary>
    [Theory]
    [InlineData(2 << 20, 1 << 20)] // fragmented: 2 MiB to a 1 MiB receiver
    [InlineData(5000, 1000)]       // single frame: 5000 bytes to a 1000-byte receiver
    public async Task OversizedInboundMessages_CloseTheConnection_AndFailTheCaller(int size, int receiverLimit)
    {
        var handler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(Factory(o => o with { MaxMessageBytes = receiverLimit }), "n0", handler);
        await using IRpcTransport client = Factory().Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        TaskCompletionSource<Exception?> closed = WatchClosed(connection);

        await Assert.ThrowsAsync<RpcConnectionException>(() => connection.CallAsync(Pattern(size, 1), Soon, Ct).WaitAsync(Soon, Ct));

        Assert.IsType<RpcConnectionException>(await closed.Task.WaitAsync(Soon, Ct));
        Assert.Equal(0, handler.Count);
        await WaitUntilAsync(() => ((IRpcTransportControl)server).OpenConnectionCount == 0, "the server dropped the offending connection");

        // The server keeps serving well-behaved peers.
        IRpcConnection again = await client.ConnectAsync(server.LocalEndpoint, Ct);
        Assert.Equal(Frame(1, 1), (await again.CallAsync(Frame(1, 1), Soon, Ct)).ToArray());
    }

    /// <summary>
    /// Connects cancelled at every stage (before start, during the QUIC handshake, while waiting for the preamble) leak
    /// nothing: no connection stays registered on either end, and disposing the transport still releases every native
    /// resource and the shared client buffer pool.
    /// </summary>
    [Fact]
    public async Task CancelledConnects_LeakNoConnectionsOrNativeResources()
    {
        QuiclyTransportFactory factory = Factory();
        await using IRpcTransport server = await StartAsync(factory, "n0", DelegateHandler.Echo());
        IRpcTransport client = factory.Create("c", AnyLoopback);
        int connected = 0, cancelled = 0;
        for (int i = 0; i < 40; i++)
        {
            // Cancelled after 0..160 ms (before the start, in the handshake, around the preamble); every tenth runs to the end.
            using var cts = i % 10 == 9 ? new CancellationTokenSource() : new CancellationTokenSource(TimeSpan.FromMilliseconds(i % 10 * 20));
            try
            {
                IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, cts.Token);
                connected++;
                Assert.Equal(Frame(1, (uint)i), (await connection.CallAsync(Frame(1, (uint)i), Soon, Ct)).ToArray());
                await connection.DisposeAsync();
            }
            catch (OperationCanceledException)
            {
                cancelled++;
            }
        }

        Assert.True(cancelled > 0, "no connect was cancelled");
        Assert.True(connected > 0, "no connect completed");
        await WaitUntilAsync(() => ((IRpcTransportControl)client).OpenConnectionCount == 0, "no client connections left");
        await WaitUntilAsync(() => ((IRpcTransportControl)server).OpenConnectionCount == 0, "no server connections left");

        await client.DisposeAsync();
        var impl = Assert.IsType<Tedd.Tdfs.Transport.Internal.QuiclyRpcTransport>(client);
        await impl.NativeResourcesReleased.WaitAsync(Soon, Ct);
        await WaitUntilAsync(() => impl.ClientPoolReleased, "client buffer pool released");
    }

    /// <summary>Non-ASCII network identities travel exactly (auth token one way, preamble the other) to both link policies.</summary>
    [Fact]
    public async Task NonAsciiNames_ReachTheLinkPolicyExactly_InBothDirections()
    {
        const string serverName = "nøde-東京-\U0001F680";
        const string clientName = "client:ç-Ω";
        var serverPolicy = new ScriptedLinkPolicy();
        var clientPolicy = new ScriptedLinkPolicy();
        var serverHandler = DelegateHandler.Echo();
        await using IRpcTransport server = await StartAsync(Factory(o => o with { LinkPolicy = serverPolicy }), serverName, serverHandler);
        await using IRpcTransport client = await StartAsync(Factory(o => o with { LinkPolicy = clientPolicy }), clientName, DelegateHandler.Echo());
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        await connection.CallAsync(Frame(1, 1), Soon, Ct);
        await serverHandler.Received.Single().Connection.CallAsync(Frame(2, 2), Soon, Ct);

        Assert.Contains((clientName, serverName), serverPolicy.ConnectChecks);
        Assert.Contains((clientName, serverName), clientPolicy.ConnectChecks);
        Assert.Contains(serverPolicy.Decisions, d => d.From == clientName && d.To == serverName && d.Direction == LinkDirection.Request);
        Assert.Contains(clientPolicy.Decisions, d => d.From == serverName && d.To == clientName && d.Direction == LinkDirection.Reply);
        Assert.Contains(clientPolicy.Decisions, d => d.From == serverName && d.To == clientName && d.Direction == LinkDirection.Request);
        ((IRpcTransportControl)server).ResetConnections(clientName);
        await WaitUntilAsync(() => !connection.IsOpen, "reset by the exact non-ASCII name");
    }

    /// <summary>
    /// Concurrent calls racing a transport-level dispose of the server: every call ends (reply or RpcConnectionException,
    /// never a wrong reply), none hangs, and the client is left without connections.
    /// </summary>
    [Fact]
    public async Task CallsRacingAServerDispose_AllEnd_WithTheRightReplyOrAConnectionError()
    {
        QuiclyTransportFactory factory = Factory();
        IRpcTransport server = await StartAsync(factory, "n0", new DelegateHandler(async (_, request, _, _) =>
        {
            if (SequenceOf(request.Span) % 5 == 0)
                await Task.Delay(1);
            return request.ToArray();
        }));
        await using IRpcTransport client = factory.Create("c", AnyLoopback);
        IRpcConnection connection = await client.ConnectAsync(server.LocalEndpoint, Ct);
        int completed = 0, failed = 0, wrong = 0;
        using var start = new ManualResetEventSlim(false);
        Task[] callers = Enumerable.Range(0, 8).Select(t => Task.Run(async () =>
        {
            start.Wait(Ct);
            for (uint i = 0; i < 400; i++)
            {
                uint sequence = (uint)(t * 1000) + i;
                try
                {
                    ReadOnlyMemory<byte> reply = await connection.CallAsync(Frame(1, sequence, (int)(sequence % 700)), Soon, Ct);
                    if (!reply.Span.SequenceEqual(Frame(1, sequence, (int)(sequence % 700))))
                        Interlocked.Increment(ref wrong);
                    Interlocked.Increment(ref completed);
                }
                catch (RpcConnectionException)
                {
                    Interlocked.Increment(ref failed);
                }
            }
        }, Ct)).ToArray();

        start.Set();
        await WaitUntilAsync(() => Volatile.Read(ref completed) > 200, "calls in flight");
        await server.DisposeAsync();
        await Task.WhenAll(callers).WaitAsync(Soon, Ct);

        Assert.Equal(0, wrong);
        Assert.Equal(8 * 400, completed + failed);
        Assert.True(failed > 0);
        Assert.False(connection.IsOpen);
        await WaitUntilAsync(() => ((IRpcTransportControl)client).OpenConnectionCount == 0, "no connections left");
    }
}
