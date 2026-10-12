using System.Collections.Concurrent;
using System.Diagnostics;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Internal.Transport.TransportTestData;

namespace Tedd.Tdfs.Tests.Unit.Internal.Transport;

/// <summary>Adversarial (review) cases for the realtime in-memory transport: close races and handler isolation.</summary>
public sealed class InMemoryTransportAdversarialTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<IRpcTransport> StartAsync(InMemoryNetwork network, string name, IRpcHandler handler)
    {
        IRpcTransport transport = network.Factory.Create(name, "mem://" + name);
        await transport.StartAsync(handler, Ct);
        return transport;
    }

    /// <summary>
    /// A handler that waits on its token (through the common <c>ct.Register</c> + TaskCompletionSource pattern) and then
    /// does slow synchronous cleanup. Cancelling the token runs the registration, and with it the rest of the handler,
    /// inline on the cancelling thread unless the transport cancels asynchronously, so closing a connection must not run
    /// that cleanup on the closer's stack (handler code never runs on the caller's stack).
    /// </summary>
    private static DelegateHandler SlowCleanupHandler(ConcurrentQueue<IRpcConnection> connections, TaskCompletionSource cleanupStarted, TimeSpan cleanup)
        => new(async (connection, _, _, ct) =>
        {
            connections.Enqueue(connection);
            var cancelled = new TaskCompletionSource();
            using (ct.Register(() => cancelled.TrySetCanceled(ct)))
            {
                try
                {
                    await cancelled.Task;
                }
                catch (OperationCanceledException)
                {
                    cleanupStarted.TrySetResult();
                    Thread.Sleep(cleanup);
                }
            }

            return null;
        });

    [Fact]
    public async Task DisposingAConnection_DoesNotRunHandlerCancellationCodeOnTheClosersStack()
    {
        var network = new InMemoryNetwork();
        var connections = new ConcurrentQueue<IRpcConnection>();
        var cleanupStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using IRpcTransport server = await StartAsync(network, "n0", SlowCleanupHandler(connections, cleanupStarted, TimeSpan.FromMilliseconds(1500)));
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);
        await WaitUntilAsync(() => !connections.IsEmpty, "request delivered");
        Assert.True(connections.TryPeek(out IRpcConnection? inbound));

        var watch = Stopwatch.StartNew();
        await inbound!.DisposeAsync();
        TimeSpan elapsed = watch.Elapsed;

        await cleanupStarted.Task.WaitAsync(Soon, Ct); // the handler's token was cancelled
        Assert.True(elapsed < TimeSpan.FromMilliseconds(750), $"DisposeAsync ran the handler's cancellation code inline ({elapsed}).");
        await Assert.ThrowsAsync<RpcConnectionException>(() => call.WaitAsync(Soon, Ct));
    }

    [Fact]
    public async Task ResettingAndDisposingTransports_DoNotRunHandlerCancellationCodeOnTheCallersStack()
    {
        var network = new InMemoryNetwork();
        var connections = new ConcurrentQueue<IRpcConnection>();
        var cleanupStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        IRpcTransport server = await StartAsync(network, "n0", SlowCleanupHandler(connections, cleanupStarted, TimeSpan.FromMilliseconds(1500)));
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");
        IRpcConnection first = await client.ConnectAsync("mem://n0", Ct);
        IRpcConnection second = await client.ConnectAsync("mem://n0", Ct);
        Task<ReadOnlyMemory<byte>> firstCall = first.CallAsync(Frame(1, 1), Timeout.InfiniteTimeSpan, Ct);
        Task<ReadOnlyMemory<byte>> secondCall = second.CallAsync(Frame(1, 2), Timeout.InfiniteTimeSpan, Ct);
        await WaitUntilAsync(() => connections.Count == 2, "requests delivered");

        var watch = Stopwatch.StartNew();
        network.ResetConnections("c");
        TimeSpan resetElapsed = watch.Elapsed;
        await Assert.ThrowsAsync<RpcConnectionException>(() => firstCall.WaitAsync(Soon, Ct));
        await Assert.ThrowsAsync<RpcConnectionException>(() => secondCall.WaitAsync(Soon, Ct));
        Assert.True(resetElapsed < TimeSpan.FromMilliseconds(750), $"ResetConnections ran handler cancellation code inline ({resetElapsed}).");

        IRpcConnection third = await client.ConnectAsync("mem://n0", Ct);
        _ = third.CallAsync(Frame(1, 3), Timeout.InfiniteTimeSpan, Ct).ContinueWith(t => t.Exception, TaskScheduler.Default);
        await WaitUntilAsync(() => connections.Count == 3, "third request delivered");
        watch.Restart();
        await server.DisposeAsync();
        Assert.True(watch.Elapsed < TimeSpan.FromMilliseconds(750), $"Transport DisposeAsync ran handler cancellation code inline ({watch.Elapsed}).");
    }

    [Fact]
    public async Task ConcurrentCallsWhileTheConnectionCloses_EachEndExactlyOnce_NoneHang()
    {
        var network = new InMemoryNetwork();
        await using IRpcTransport server = await StartAsync(network, "n0", new DelegateHandler(async (_, request, _, _) =>
        {
            if (SequenceOf(request.Span) % 3 == 0)
                await Task.Yield();
            return request.ToArray();
        }));
        await using IRpcTransport client = network.Factory.Create("c", "mem://c");

        for (int round = 0; round < 20; round++)
        {
            IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
            int completed = 0, failed = 0, wrong = 0;
            using var start = new ManualResetEventSlim(false);
            Task[] callers = Enumerable.Range(0, 8).Select(t => Task.Run(async () =>
            {
                start.Wait(Ct);
                for (uint i = 0; i < 200; i++)
                {
                    uint sequence = (uint)(t * 1000) + i;
                    try
                    {
                        ReadOnlyMemory<byte> reply = await connection.CallAsync(Frame(1, sequence), Soon, Ct);
                        if (SequenceOf(reply.Span) != sequence)
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
            await Task.Delay(round % 4, Ct);
            await connection.DisposeAsync();
            await Task.WhenAll(callers).WaitAsync(Soon, Ct); // a call that was never failed nor answered would hang here

            Assert.Equal(0, wrong);
            Assert.Equal(8 * 200, completed + failed);
        }
    }

    [Fact]
    public async Task RemoteClose_DeliversEveryMessageSentBeforeIt_EvenWhileTheReceiverIsBusy()
    {
        var network = new InMemoryNetwork();
        using var gate = new ManualResetEventSlim(false);
        var serverConnections = new ConcurrentQueue<IRpcConnection>();
        var serverHandler = new DelegateHandler((connection, request, _, _) =>
        {
            serverConnections.Enqueue(connection);
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(Frame(2, SequenceOf(request.Span)));
        });
        await using IRpcTransport server = await StartAsync(network, "n0", serverHandler);
        var clientHandler = new DelegateHandler((_, request, _, _) =>
        {
            if (TypeOf(request.Span) == 9)
                gate.Wait(Soon); // stalls the client's inbound queue
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);
        });
        await using IRpcTransport client = await StartAsync(network, "c", clientHandler);
        IRpcConnection connection = await client.ConnectAsync("mem://n0", Ct);
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Closed += (_, ex) => closed.TrySetResult(ex);
        await connection.CallAsync(Frame(1, 0), Soon, Ct);
        Assert.True(serverConnections.TryPeek(out IRpcConnection? inbound));

        await inbound!.SendAsync(Frame(9, 0), Ct);                  // client handler blocks on this one
        await WaitUntilAsync(() => clientHandler.Count == 1, "client stalled");
        Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Frame(1, 7), Soon, Ct);
        await WaitUntilAsync(() => serverHandler.Count == 2, "request served");  // its reply now waits behind the stall
        for (uint i = 0; i < 5; i++)
            await inbound.SendAsync(Frame(10, i), Ct);
        await inbound.DisposeAsync();
        await Task.Delay(100, Ct);
        gate.Set();

        Assert.Equal(Frame(2, 7), (await call.WaitAsync(Soon, Ct)).ToArray());
        await WaitUntilAsync(() => clientHandler.Received.Count(r => TypeOf(r.Payload) == 10) == 5, "one-way messages sent before the close");
        Assert.IsType<RpcConnectionException>(await closed.Task.WaitAsync(Soon, Ct));
    }
}
