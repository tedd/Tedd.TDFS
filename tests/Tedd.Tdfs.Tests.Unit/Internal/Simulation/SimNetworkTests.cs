using Tedd.Tdfs.Testing.Simulation;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Internal.Simulation.SimText;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>Tests of <see cref="SimNetwork"/>: IRpcTransport contracts, ordering, link policy, timeouts, close/reset/crash.</summary>
public sealed class SimNetworkTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    private static (SimScheduler Scheduler, SimNetwork Network) Create(ulong seed = 1, SimNetworkOptions? options = null, ILinkPolicy? policy = null, SimSchedulerOptions? schedulerOptions = null)
    {
        var scheduler = new SimScheduler(seed, schedulerOptions);
        return (scheduler, new SimNetwork(scheduler, options, policy));
    }

    private static async Task<IRpcTransport> ListenAsync(SimNetwork network, string name, IRpcHandler handler)
    {
        IRpcTransport transport = network.Create(name, "mem://" + name);
        await transport.StartAsync(handler);
        return transport;
    }

    [Fact]
    public void Options_AreValidated()
    {
        var s = new SimScheduler(1);
        Assert.Throws<ArgumentException>(() => new SimNetwork(s, new SimNetworkOptions { MinLatency = -Ms }));
        Assert.Throws<ArgumentException>(() => new SimNetwork(s, new SimNetworkOptions { MinLatency = 2 * Ms, MaxLatency = Ms }));
        Assert.Throws<ArgumentException>(() => new SimNetwork(s, new SimNetworkOptions { BlockedConnectTimeout = TimeSpan.Zero }));
        Assert.Throws<ArgumentNullException>(() => new SimNetwork(null!));
    }

    [Fact]
    public void Create_ResolvesEndpointsAndOwners()
    {
        (SimScheduler s, SimNetwork net) = Create();
        IRpcTransport a = net.Create("n0", "mem://n0");
        IRpcTransport b = net.Create("client:c0", "");
        IRpcTransport c = net.Create("q", "quic://127.0.0.1:0");
        IRpcTransport d = net.Create("q2", "quic://127.0.0.1:0");
        Assert.Equal("mem://n0", a.LocalEndpoint);
        Assert.Equal("mem://client:c0", b.LocalEndpoint);
        Assert.StartsWith("quic://127.0.0.1:", c.LocalEndpoint, StringComparison.Ordinal);
        Assert.NotEqual(c.LocalEndpoint, d.LocalEndpoint);
        Assert.NotNull(s.FindProcess("n0"));
        Assert.NotNull(s.FindProcess("client:c0"));
        Assert.Throws<ArgumentException>(() => net.Create("x", "ftp://nope"));
        Assert.Throws<ArgumentException>(() => net.Create(" ", "mem://x"));
        Assert.Throws<ArgumentException>(() => net.Create("has space", ""));
        Assert.Same(net, net.Factory);
    }

    [Fact]
    public async Task CallAsync_RoundTripsWithinTheLatencyBounds()
    {
        (SimScheduler s, SimNetwork net) = Create(options: new SimNetworkOptions { MinLatency = Ms, MaxLatency = 3 * Ms });
        DelegateHandler handler = DelegateHandler.Echo();
        (string Reply, TimeSpan Elapsed) result = await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcTransport client = net.Create("c", "");
            IRpcConnection connection = await client.ConnectAsync("mem://n1");
            Assert.True(connection.IsOpen);
            Assert.Equal(RpcAddress.Parse("mem://n1"), connection.Remote);
            TimeSpan start = s.Now;
            ReadOnlyMemory<byte> reply = await connection.CallAsync(Bytes("hello"), Second);
            return (Str(reply), s.Now - start);
        });

        Assert.Equal("re:hello", result.Reply);
        Assert.InRange(result.Elapsed, 2 * Ms, 6 * Ms);
        Assert.Equal([("hello", true)], handler.Received);
        Assert.Equal(RpcAddress.Parse("mem://c"), Assert.Single(handler.Connections).Remote);
        Assert.Equal(2, net.MessagesSent);
        Assert.Equal(2, net.MessagesDelivered);
        Assert.Equal(1, net.ConnectionsOpened);
        Assert.Equal(["mem://n1"], net.ListeningEndpoints);
        Assert.Equal("hello".Length + "re:hello".Length, net.BytesSent);
    }

    [Fact]
    public async Task Handler_RunsAsBackgroundWorkOfTheServerProcess()
    {
        (SimScheduler s, SimNetwork net) = Create();
        (string? Process, bool NoContext) seen = default;
        var handler = new DelegateHandler((_, _, _, _) =>
        {
            seen = (s.CurrentProcess?.Id, SynchronizationContext.Current is null);
            return new ValueTask<ReadOnlyMemory<byte>?>(Bytes("ok"));
        });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await connection.CallAsync(Bytes("x"), Second);
        });
        Assert.Equal(("n1#1", true), seen);
    }

    [Fact]
    public async Task MessagesOnOneConnection_ArriveInSendOrder()
    {
        (SimScheduler s, SimNetwork net) = Create(seed: 5, new SimNetworkOptions { MinLatency = TimeSpan.Zero, MaxLatency = 5 * Ms });
        DelegateHandler handler = DelegateHandler.Echo();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            for (int i = 0; i < 50; i++)
                await connection.SendAsync(Bytes(i.ToString("D2", System.Globalization.CultureInfo.InvariantCulture)));
            await s.DelayAsync(Second);
        });

        Assert.Equal(Enumerable.Range(0, 50).Select(i => i.ToString("D2", System.Globalization.CultureInfo.InvariantCulture)), handler.Received.Select(r => r.Text));
        Assert.All(handler.Received, r => Assert.False(r.ExpectsReply));
    }

    [Fact]
    public async Task MessagesOnDifferentConnections_CanBeReordered()
    {
        bool reordered = false;
        for (ulong seed = 1; seed <= 30 && !reordered; seed++)
        {
            (SimScheduler s, SimNetwork net) = Create(seed, new SimNetworkOptions { MinLatency = TimeSpan.Zero, MaxLatency = 5 * Ms });
            DelegateHandler handler = DelegateHandler.Echo();
            await s.RunAsync(async () =>
            {
                await ListenAsync(net, "n1", handler);
                IRpcConnection first = await net.Create("c1", "").ConnectAsync("mem://n1");
                IRpcConnection second = await net.Create("c2", "").ConnectAsync("mem://n1");
                await first.SendAsync(Bytes("first"));
                await second.SendAsync(Bytes("second"));
                await s.DelayAsync(Second);
            });
            reordered = handler.Received[0].Text == "second";
        }

        Assert.True(reordered, "independent connections should be able to deliver out of send order");
    }

    [Fact]
    public async Task DroppedRequest_TimesOutOnTheCallersClock()
    {
        var policy = new DelegateLinkPolicy { OnDecide = (_, _, d, _) => d == LinkDirection.Request ? LinkDecision.Drop : LinkDecision.Deliver };
        (SimScheduler s, SimNetwork net) = Create(policy: policy);
        s.CreateClock("c", new SimClockOptions { Rate = 2.0 }); // the caller's clock runs twice as fast
        DelegateHandler handler = DelegateHandler.Echo();
        (Exception Error, TimeSpan Elapsed) result = await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            TimeSpan start = s.Now;
            Exception error = await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Bytes("lost"), Second));
            return (error, s.Now - start);
        });

        Assert.Equal(TimeSpan.FromMilliseconds(500), result.Elapsed);
        Assert.Contains("may have been executed", result.Error.Message, StringComparison.Ordinal);
        Assert.Empty(handler.Received);
        Assert.Equal(1, net.MessagesDropped);
    }

    [Fact]
    public async Task DroppedReply_ExecutesTheRequestButTheCallerTimesOut()
    {
        // HAR-006: request and reply directions are distinct.
        var policy = new DelegateLinkPolicy { OnDecide = (from, _, d, _) => d == LinkDirection.Reply && from == "n1" ? LinkDecision.Drop : LinkDecision.Deliver };
        (SimScheduler s, SimNetwork net) = Create(policy: policy);
        DelegateHandler handler = DelegateHandler.Echo();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Bytes("executed"), 100 * Ms));
        });

        Assert.Equal([("executed", true)], handler.Received);
        Assert.Equal([("c", "n1", LinkDirection.Request, "executed"), ("n1", "c", LinkDirection.Reply, "re:executed")], policy.Seen);
    }

    [Fact]
    public async Task DuplicateDecision_DeliversTwice_AndTheSecondReplyIsLate()
    {
        var policy = new DelegateLinkPolicy { OnDecide = (_, _, d, _) => d == LinkDirection.Request ? LinkDecision.Duplicate : LinkDecision.Deliver };
        (SimScheduler s, SimNetwork net) = Create(policy: policy);
        DelegateHandler handler = DelegateHandler.Echo();
        string reply = await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            string text = Str(await connection.CallAsync(Bytes("twice"), Second));
            await s.DelayAsync(Second);
            return text;
        });

        Assert.Equal("re:twice", reply);
        Assert.Equal(2, handler.Received.Count);
        Assert.Equal(1, net.MessagesDuplicated);
        Assert.Equal(1, net.LateReplies);
    }

    [Fact]
    public async Task DelayDecision_DelaysTheMessage_AndLaterMessagesMayOvertakeIt()
    {
        var policy = new DelegateLinkPolicy
        {
            OnDecide = (_, _, _, payload) => Str(payload) == "slow" ? LinkDecision.Delayed(10 * Ms) : LinkDecision.Deliver,
        };
        (SimScheduler s, SimNetwork net) = Create(policy: policy, options: new SimNetworkOptions { MinLatency = Ms, MaxLatency = Ms });
        var arrivals = new List<(string, TimeSpan)>();
        var handler = new DelegateHandler((_, request, _, _) =>
        {
            arrivals.Add((Str(request), s.Now));
            return new ValueTask<ReadOnlyMemory<byte>?>((ReadOnlyMemory<byte>?)null);
        });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            TimeSpan sentAt = s.Now;
            await connection.SendAsync(Bytes("slow"));
            await connection.SendAsync(Bytes("fast"));
            await s.DelayAsync(Second);
            Assert.Equal([("fast", sentAt + Ms), ("slow", sentAt + 11 * Ms)], arrivals);
        });
        Assert.Equal(1, net.MessagesDelayed);
    }

    [Fact]
    public async Task BlockedConnect_FailsAfterTheConfiguredTimeout()
    {
        var policy = new DelegateLinkPolicy { OnConnect = (_, to) => to != "n1" };
        (SimScheduler s, SimNetwork net) = Create(policy: policy, options: new SimNetworkOptions { BlockedConnectTimeout = 3 * Second });
        TimeSpan elapsed = await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            TimeSpan start = s.Now;
            await Assert.ThrowsAsync<RpcConnectionException>(() => net.Create("c", "").ConnectAsync("mem://n1"));
            return s.Now - start;
        });
        Assert.Equal(3 * Second, elapsed);
        Assert.Equal(1, net.ConnectionsFailed);
    }

    [Fact]
    public async Task ConnectToNothing_IsRefused_AndBadEndpointsAreArgumentErrors()
    {
        (SimScheduler s, SimNetwork net) = Create();
        await s.RunAsync(async () =>
        {
            IRpcTransport client = net.Create("c", "");
            RpcConnectionException refused = await Assert.ThrowsAsync<RpcConnectionException>(() => client.ConnectAsync("mem://nobody"));
            Assert.Contains("refused", refused.Message, StringComparison.Ordinal);
            Assert.Throws<ArgumentException>(() => { _ = client.ConnectAsync("not an endpoint"); });
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ConnectAsync("mem://nobody", cts.Token));
        });
    }

    [Fact]
    public async Task ConnectCancelledInFlight_ClosesTheServerSide()
    {
        (SimScheduler s, SimNetwork net) = Create(options: new SimNetworkOptions { MinLatency = 5 * Ms, MaxLatency = 5 * Ms });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            using var cts = new CancellationTokenSource(7 * Ms, s.Clock); // after the request arrives, before the answer
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => net.Create("c", "").ConnectAsync("mem://n1", cts.Token));
            await s.DelayAsync(Second);
        });
        Assert.Equal(0, net.OpenConnectionCount);
    }

    [Fact]
    public async Task StartTwiceOnOneEndpoint_IsAddressInUse_UntilTheFirstIsDisposed()
    {
        (SimScheduler s, SimNetwork net) = Create();
        await s.RunAsync(async () =>
        {
            IRpcTransport first = await ListenAsync(net, "n1", DelegateHandler.Echo());
            IRpcTransport second = net.Create("n1b", "mem://n1");
            await Assert.ThrowsAsync<RpcConnectionException>(() => second.StartAsync(DelegateHandler.Echo()));
            await Assert.ThrowsAsync<InvalidOperationException>(() => first.StartAsync(DelegateHandler.Echo()));
            await first.DisposeAsync();
            await second.StartAsync(DelegateHandler.Echo());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => first.StartAsync(DelegateHandler.Echo()));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => first.ConnectAsync("mem://n1"));
        });
    }

    [Fact]
    public async Task Dispose_ClosesBothEnds_FailsPendingCalls_AndCancelsHandlers()
    {
        (SimScheduler s, SimNetwork net) = Create();
        CancellationToken handlerToken = default;
        var serverClosed = new List<(TimeSpan, Exception?)>();
        var clientClosed = new List<Exception?>();
        var never = new TaskCompletionSource<ReadOnlyMemory<byte>?>();
        var handler = new DelegateHandler((connection, _, _, ct) =>
        {
            handlerToken = ct;
            connection.Closed += (_, e) => serverClosed.Add((s.Now, e));
            return new ValueTask<ReadOnlyMemory<byte>?>(never.Task);
        });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            connection.Closed += (_, e) => clientClosed.Add(e);
            Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Bytes("wait"), Timeout.InfiniteTimeSpan);
            await s.DelayAsync(10 * Ms);
            TimeSpan closedAt = s.Now;
            await connection.DisposeAsync();
            Assert.False(connection.IsOpen);
            await Assert.ThrowsAsync<RpcConnectionException>(() => pending);
            await Assert.ThrowsAsync<RpcConnectionException>(() => connection.CallAsync(Bytes("again"), Second));
            await Assert.ThrowsAsync<RpcConnectionException>(async () => await connection.SendAsync(Bytes("again")));
            await s.DelayAsync(Second);
            Assert.True(Assert.Single(serverClosed).Item1 > closedAt);
        });

        Assert.Equal([(Exception?)null], clientClosed);
        Assert.Null(Assert.Single(serverClosed).Item2);
        Assert.True(handlerToken.IsCancellationRequested);
        Assert.Equal(0, net.OpenConnectionCount);
    }

    [Fact]
    public async Task ResetConnections_ClosesBothEndsWithAnError()
    {
        (SimScheduler s, SimNetwork net) = Create();
        var errors = new List<Exception?>();
        var handler = new DelegateHandler((connection, _, _, _) =>
        {
            connection.Closed += (_, e) => errors.Add(e);
            return new ValueTask<ReadOnlyMemory<byte>?>(new TaskCompletionSource<ReadOnlyMemory<byte>?>().Task);
        });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            connection.Closed += (_, e) => errors.Add(e);
            Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Bytes("x"), Second);
            await s.DelayAsync(10 * Ms);
            net.ResetConnections("n1");
            await Assert.ThrowsAsync<RpcConnectionException>(() => pending);
            await s.DelayAsync(Ms);
        });

        Assert.Equal(2, errors.Count);
        Assert.All(errors, e => Assert.IsType<RpcConnectionException>(e));
    }

    [Fact]
    public async Task KillingTheServerProcess_ResetsPeers_AndFreesTheEndpoint()
    {
        (SimScheduler s, SimNetwork net) = Create();
        SimNodeLoop loop = s.LoopFactory.Create("n1");
        var never = new TaskCompletionSource<ReadOnlyMemory<byte>?>();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", new DelegateHandler((_, _, _, _) => new ValueTask<ReadOnlyMemory<byte>?>(never.Task)));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Bytes("x"), TimeSpan.FromMinutes(1));
            await s.DelayAsync(10 * Ms);
            TimeSpan killedAt = s.Now;
            loop.Stop(); // crash the node process
            RpcConnectionException reset = await Assert.ThrowsAsync<RpcConnectionException>(() => pending);
            Assert.Contains("n1#1 died", reset.Message, StringComparison.Ordinal);
            Assert.True(s.Now - killedAt < Second);
            Assert.Empty(net.ListeningEndpoints);

            // A new incarnation can listen on the same endpoint and serve.
            SimNodeLoop restarted = s.LoopFactory.Create("n1");
            Assert.Equal("n1#2", restarted.Process.Id);
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            IRpcConnection again = await net.Create("c", "").ConnectAsync("mem://n1");
            Assert.Equal("re:hi", Str(await again.CallAsync(Bytes("hi"), Second)));
        });
    }

    [Fact]
    public async Task KillingTheServerProcess_WithoutResets_LeavesCallsToTimeOut()
    {
        (SimScheduler s, SimNetwork net) = Create(options: new SimNetworkOptions { ResetPeersOnProcessDeath = false });
        SimNodeLoop loop = s.LoopFactory.Create("n1");
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", new DelegateHandler((_, _, _, _) => new ValueTask<ReadOnlyMemory<byte>?>(new TaskCompletionSource<ReadOnlyMemory<byte>?>().Task)));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            Task<ReadOnlyMemory<byte>> pending = connection.CallAsync(Bytes("x"), Second);
            loop.Stop();
            await Assert.ThrowsAsync<TimeoutException>(() => pending);
            Assert.True(connection.IsOpen); // nobody told the client
            await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Bytes("y"), Second));
        });
    }

    [Fact]
    public async Task PausedServer_HoldsRequests_TheCallTimesOut_AndTheLateReplyIsCounted()
    {
        (SimScheduler s, SimNetwork net) = Create();
        SimNodeLoop loop = s.LoopFactory.Create("n1");
        DelegateHandler handler = DelegateHandler.Echo();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            loop.Pause();
            await Assert.ThrowsAsync<TimeoutException>(() => connection.CallAsync(Bytes("frozen"), 100 * Ms));
            Assert.Empty(handler.Received);
            loop.Resume();
            await s.DelayAsync(Second);
        });

        Assert.Equal([("frozen", true)], handler.Received);
        Assert.Equal(1, net.LateReplies);
    }

    [Fact]
    public async Task CallAsync_Cancellation_CompletesAsCancelled()
    {
        (SimScheduler s, SimNetwork net) = Create();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", new DelegateHandler((_, _, _, _) => new ValueTask<ReadOnlyMemory<byte>?>(new TaskCompletionSource<ReadOnlyMemory<byte>?>().Task)));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            using var cts = new CancellationTokenSource();
            Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Bytes("x"), Second, cts.Token);
            await s.DelayAsync(Ms);
            cts.Cancel();
            Assert.True(call.IsCanceled);
            Assert.True((await Task.WhenAny(call)).IsCanceled);
            using var precanceled = new CancellationTokenSource();
            precanceled.Cancel();
            Assert.True(connection.CallAsync(Bytes("y"), Second, precanceled.Token).IsCanceled);
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = connection.CallAsync(Bytes("z"), TimeSpan.Zero); });
            Assert.True(connection.IsOpen);
        });
    }

    [Fact]
    public async Task HandlerFailure_IsReported_AndTheConnectionIsReset()
    {
        (SimScheduler s, SimNetwork net) = Create(schedulerOptions: new SimSchedulerOptions { ThrowOnUnhandledException = false });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", new DelegateHandler((_, _, _, _) => throw new FormatException("bad handler")));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await Assert.ThrowsAsync<RpcConnectionException>(() => connection.CallAsync(Bytes("x"), Second));
            Assert.False(connection.IsOpen);
        });

        SimUnhandledError error = Assert.Single(s.UnhandledExceptions);
        Assert.IsType<FormatException>(error.Exception);
        Assert.Contains("RPC handler of n1", error.Lane, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HandlerFailure_FailsTheRunByDefault()
    {
        (SimScheduler s, SimNetwork net) = Create();
        await Assert.ThrowsAsync<SimulationException>(() => s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", new DelegateHandler((_, _, _, _) => throw new FormatException("bad handler")));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await connection.CallAsync(Bytes("x"), Second);
        }));
    }

    [Fact]
    public async Task RequestToATransportThatNeverStarted_ResetsTheConnection()
    {
        (SimScheduler s, SimNetwork net) = Create();
        IRpcConnection? serverSide = null;
        var handler = new DelegateHandler((connection, _, _, _) =>
        {
            serverSide = connection;
            return new ValueTask<ReadOnlyMemory<byte>?>(Bytes("ok"));
        });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await connection.CallAsync(Bytes("hello"), Second);
            await Assert.ThrowsAsync<RpcConnectionException>(() => serverSide!.CallAsync(Bytes("callback"), Second));
        });
    }

    [Fact]
    public async Task ServerCanCallBackOverTheSameConnection_WhenTheClientListens()
    {
        (SimScheduler s, SimNetwork net) = Create();
        IRpcConnection? serverSide = null;
        var server = new DelegateHandler((connection, _, _, _) =>
        {
            serverSide = connection;
            return new ValueTask<ReadOnlyMemory<byte>?>(Bytes("ok"));
        });
        string reply = await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", server);
            IRpcTransport client = await ListenAsync(net, "n2", DelegateHandler.Echo());
            IRpcConnection connection = await client.ConnectAsync("mem://n1");
            await connection.CallAsync(Bytes("hello"), Second);
            return Str(await serverSide!.CallAsync(Bytes("callback"), Second));
        });
        Assert.Equal("re:callback", reply);
    }

    [Fact]
    public async Task Bandwidth_DelaysLargeMessages()
    {
        (SimScheduler s, SimNetwork net) = Create();
        net.SetLatency("c", "n1", TimeSpan.Zero);
        net.SetBandwidth("c", "n1", 1_000_000);
        Assert.Equal(new SimLinkSettings(TimeSpan.Zero, TimeSpan.Zero, 1_000_000), net.GetLink("c", "n1"));
        var arrivals = new List<TimeSpan>();
        var handler = new DelegateHandler((_, _, _, _) =>
        {
            arrivals.Add(s.Now);
            return new ValueTask<ReadOnlyMemory<byte>?>((ReadOnlyMemory<byte>?)null);
        });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            TimeSpan start = s.Now;
            await connection.SendAsync(new byte[100_000]);
            await connection.SendAsync(new byte[100_000]);
            await s.DelayAsync(Second);
            Assert.Equal([start + 100 * Ms, start + 200 * Ms], arrivals);
        });

        net.ClearLink("c", "n1");
        Assert.Equal(net.Options.MinLatency, net.GetLink("c", "n1").MinLatency);
        net.SetLink("a", "b", new SimLinkSettings(Ms, 2 * Ms));
        net.ClearLinks();
        Assert.Equal(0, net.GetLink("a", "b").BytesPerSecond);
        Assert.Throws<ArgumentException>(() => net.SetLink("a", "b", new SimLinkSettings(2 * Ms, Ms)));
    }

    [Fact]
    public async Task SdkStyleCode_WithConfigureAwaitFalse_StaysOnTheSimulationThread()
    {
        (SimScheduler s, SimNetwork net) = Create();
        var threads = new List<bool>();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            IRpcTransport client = net.Create("client:c0", "");
            IRpcConnection connection = await client.ConnectAsync("mem://n1").ConfigureAwait(false);
            threads.Add(s.IsOnSimulationThread);
            for (int i = 0; i < 5; i++)
            {
                await connection.CallAsync(Bytes("x"), Second).ConfigureAwait(false);
                threads.Add(s.IsOnSimulationThread && SynchronizationContext.Current is null);
                await Task.Delay(Ms, s.Clock).ConfigureAwait(false);
            }

            await connection.DisposeAsync().ConfigureAwait(false);
        });

        Assert.Equal(6, threads.Count);
        Assert.All(threads, Assert.True);
        Assert.Empty(s.Violations);
    }

    [Fact]
    public async Task MessageSent_ReportsEveryMessageWithItsDecision()
    {
        var policy = new DelegateLinkPolicy { OnDecide = (_, _, d, _) => d == LinkDirection.OneWay ? LinkDecision.BlackHole : LinkDecision.Deliver };
        (SimScheduler s, SimNetwork net) = Create(policy: policy);
        var events = new List<SimMessageEvent>();
        net.MessageSent += events.Add;
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await connection.CallAsync(Bytes("q"), Second);
            await connection.SendAsync(Bytes("gone"));
            await s.DelayAsync(Second);
        });

        Assert.Equal(
            [(LinkDirection.Request, "c", "n1", "q", LinkDecisionKind.Deliver), (LinkDirection.Reply, "n1", "c", "re:q", LinkDecisionKind.Deliver), (LinkDirection.OneWay, "c", "n1", "gone", LinkDecisionKind.BlackHole)],
            events.Select(e => (e.Direction, e.From, e.To, Str(e.Payload), e.Decision.Kind)));
        Assert.Equal(1, net.MessagesDropped);
        Assert.Same(policy, net.Policy);
        net.Policy = null!;
        Assert.Same(AllowAllLinkPolicy.Instance, net.Policy);
    }

    [Fact]
    public async Task Identity_IsSettablePerEnd()
    {
        (SimScheduler s, SimNetwork net) = Create();
        IRpcConnection? serverSide = null;
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", new DelegateHandler((connection, _, _, _) =>
            {
                serverSide = connection;
                connection.Identity = new RpcPeerIdentity(null, null, 0, "alice", IsNode: false, IsBackup: false);
                return new ValueTask<ReadOnlyMemory<byte>?>(Bytes("ok"));
            }));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await connection.CallAsync(Bytes("x"), Second);
            Assert.Null(connection.Identity);
        });
        Assert.Equal("alice", serverSide!.Identity!.Principal);
    }
}
