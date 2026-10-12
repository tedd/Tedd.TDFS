using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Simulation;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Internal.Simulation.SimText;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>
/// Adversarial regression tests from the review of the simulation runtime: clock freezing, connection teardown from
/// simulated contexts, processes dying mid-handshake or mid-call, re-entrant runs and whole-scenario replay with crashes.
/// </summary>
public sealed class SimReviewTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    private static async Task<IRpcTransport> ListenAsync(SimNetwork network, string name, IRpcHandler handler)
    {
        IRpcTransport transport = network.Create(name, "mem://" + name);
        await transport.StartAsync(handler);
        return transport;
    }

    private static DelegateHandler NeverReplies() =>
        new((_, _, _, _) => new ValueTask<ReadOnlyMemory<byte>?>(new TaskCompletionSource<ReadOnlyMemory<byte>?>().Task));

    [Fact]
    public void FrozenClock_Advance_FiresTimersThatBecameDue()
    {
        // SimTimeProvider.Advance documents "timers that become due fire at the current virtual instant"; that must hold
        // for a frozen clock too (freeze + manual stepping is the canonical HAR-003 clock-control pattern).
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        var fired = new List<(string Name, TimeSpan At)>();
        n0.CreateTimer(_ => fired.Add(("ten", s.Now)), null, 10 * Second, Timeout.InfiniteTimeSpan);
        n0.CreateTimer(_ => fired.Add(("twenty", s.Now)), null, 20 * Second, Timeout.InfiniteTimeSpan);
        s.RunFor(Second);

        n0.Freeze();
        n0.Advance(10 * Second); // local reading 11 s: "ten" is due, "twenty" is not
        s.RunFor(5 * Second);
        Assert.Equal([("ten", Second)], fired);
        Assert.Equal(TimeSpan.FromSeconds(11), n0.Elapsed);

        n0.Unfreeze(); // at global 6 s; "twenty" needs 9 more local seconds
        s.RunUntilIdle();
        Assert.Equal([("ten", Second), ("twenty", TimeSpan.FromSeconds(15))], fired);
    }

    [Fact]
    public void FrozenClock_StillFiresTimersThatAreAlreadyDue_ButNothingLater()
    {
        // A frozen reading that has reached a timer's due time makes it due (TestClock behaves the same way); only timers
        // that are not yet due are held. A periodic timer fires once and is then held: no livelock.
        var s = new SimScheduler(1, new SimSchedulerOptions { Mode = SimScheduleMode.Fifo });
        SimTimeProvider n0 = s.CreateClock("n0");
        var fired = new List<(string Name, TimeSpan At)>();
        n0.CreateTimer(_ => fired.Add(("before-freeze", s.Now)), null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        n0.Freeze();
        n0.CreateTimer(_ => fired.Add(("while-frozen", s.Now)), null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        n0.CreateTimer(_ => fired.Add(("periodic", s.Now)), null, TimeSpan.Zero, Ms);
        n0.CreateTimer(_ => fired.Add(("later", s.Now)), null, Ms, Timeout.InfiniteTimeSpan);

        s.RunFor(Second);
        Assert.Equal([("before-freeze", TimeSpan.Zero), ("while-frozen", TimeSpan.Zero), ("periodic", TimeSpan.Zero)], fired);

        n0.Unfreeze();
        s.RunFor(2 * Ms);
        Assert.Equal(
            [("periodic", Second + Ms), ("later", Second + Ms), ("periodic", Second + 2 * Ms)],
            fired.Skip(3));
    }

    [Fact]
    public async Task TaskDelayZero_OnAFrozenClock_DoesNotHang()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        n0.Freeze();
        bool done = await s.RunAsync(async () =>
        {
            using var cts = new CancellationTokenSource();
            ITimer immediate = n0.CreateTimer(_ => cts.Cancel(), null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task.Delay(Timeout.InfiniteTimeSpan, s.Clock, cts.Token));
            await immediate.DisposeAsync();
            return true;
        }, TimeSpan.FromSeconds(5));
        Assert.True(done);
    }

    [Fact]
    public async Task ClosingAConnectionOnAMainLane_KeepsHandlerAndClosedContinuationsOnTheSimulationThread()
    {
        // The handler token and the Closed event complete SDK-style (ConfigureAwait(false)) awaiters. Closing from a
        // simulated main lane (a node loop or the client context) must not push those continuations to the thread pool.
        var s = new SimScheduler(1, new SimSchedulerOptions { ThrowOnViolation = false });
        var net = new SimNetwork(s);
        int simThread = 0;
        var observed = new List<string>();
        IRpcConnection? serverSide = null;
        var closedSignal = new TaskCompletionSource();

        bool OnSimThread() => s.IsOnSimulationThread && Environment.CurrentManagedThreadId == simThread;

        async ValueTask<ReadOnlyMemory<byte>?> HoldUntilCancelledAsync(IRpcConnection connection, CancellationToken ct)
        {
            serverSide = connection;
            connection.Closed += (_, _) => closedSignal.TrySetResult();

            // An SDK-style cancellable wait (a completion source tied to the token, as AsyncGate-like primitives do; a
            // cancelled Task.Delay never inlines, see CancelledTaskDelay_AlwaysEscapesToThePool_AndIsReported).
            var cancelled = new TaskCompletionSource();
            using CancellationTokenRegistration registration = ct.UnsafeRegister(static state => ((TaskCompletionSource)state!).TrySetResult(), cancelled);
            await cancelled.Task.ConfigureAwait(false);

            lock (observed)
                observed.Add("handler:" + OnSimThread());
            return null;
        }

        async Task WatchCloseAsync()
        {
            await closedSignal.Task.ConfigureAwait(false);
            lock (observed)
                observed.Add("closed:" + OnSimThread());
        }

        await s.RunAsync(async () =>
        {
            simThread = Environment.CurrentManagedThreadId;
            await ListenAsync(net, "n1", new DelegateHandler((connection, _, _, ct) => HoldUntilCancelledAsync(connection, ct)));
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            Task<ReadOnlyMemory<byte>> call = connection.CallAsync(Bytes("hold"), Second);
            await s.DelayAsync(10 * Ms);
            Task watcher = WatchCloseAsync();

            await serverSide!.DisposeAsync(); // runs on the client main lane (custom synchronization context)

            lock (observed)
                Assert.Equal(["handler:True", "closed:True"], observed);
            Assert.True(watcher.IsCompletedSuccessfully);
            await Assert.ThrowsAsync<RpcConnectionException>(() => call);
        });

        Assert.Empty(s.Violations);
    }

    [Fact]
    public void CancelledTaskDelay_AlwaysEscapesToThePool_AndIsReported()
    {
        // Runtime behaviour the SDK must design around (CLAUDE.md gotcha): a Task.Delay completed by its cancellation token
        // runs ConfigureAwait(false) continuations on the thread pool even when the cancelling thread has no
        // synchronization context. The simulation cannot prevent it, but it must report it (HAR-002).
        var s = new SimScheduler(1, new SimSchedulerOptions { ThrowOnViolation = false, StallGracePeriod = TimeSpan.FromSeconds(10) });
        using var cts = new CancellationTokenSource();
        bool continued = false;
        bool onSimulationThread = true;

        async Task SdkStyleBackoffAsync()
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(1), s.Clock, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            onSimulationThread = s.IsOnSimulationThread;
            s.Post(() => continued = true);
        }

        s.Post(() => _ = SdkStyleBackoffAsync());
        s.Schedule(Ms, cts.Cancel); // a background (no-context) item cancels the backoff
        s.RunUntil(() => continued, TimeSpan.FromMinutes(5));

        Assert.False(onSimulationThread);
        Assert.NotEmpty(s.Violations);
        Assert.All(s.Violations, v => Assert.True(v.IsThreadPoolThread));
    }

    [Fact]
    public async Task ConnectFromAProcessThatDiesInFlight_LeavesNoHalfOpenConnection()
    {
        var s = new SimScheduler(1);
        var net = new SimNetwork(s, new SimNetworkOptions { MinLatency = 5 * Ms, MaxLatency = 5 * Ms });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            IRpcTransport client = net.Create("c", "");
            Task<IRpcConnection> connect = client.ConnectAsync("mem://n1");
            await s.DelayAsync(2 * Ms);
            s.FindProcess("c")!.Kill(); // crashes before its connection request reaches n1
            await s.DelayAsync(Second);
            Assert.False(connect.IsCompleted); // the connecting code is dead: nobody observes the outcome
        });

        Assert.Equal(0, net.ConnectionsOpened);
        Assert.Equal(0, net.OpenConnectionCount);
    }

    [Fact]
    public async Task ConnectOnATransportDisposedInFlight_Fails_AndLeavesNoConnection()
    {
        var s = new SimScheduler(1);
        var net = new SimNetwork(s, new SimNetworkOptions { MinLatency = 5 * Ms, MaxLatency = 5 * Ms });
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", DelegateHandler.Echo());
            IRpcTransport client = net.Create("c", "");
            Task<IRpcConnection> beforeArrival = client.ConnectAsync("mem://n1");
            await s.DelayAsync(2 * Ms);
            IRpcTransport other = net.Create("d", "");
            Task<IRpcConnection> afterArrival = other.ConnectAsync("mem://n1");
            await s.DelayAsync(7 * Ms); // "d"'s request has arrived (pair created), its answer is still in flight

            await client.DisposeAsync();
            await other.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => beforeArrival);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => afterArrival);
            await s.DelayAsync(Second);
        });

        Assert.Equal(0, net.OpenConnectionCount);
    }

    [Fact]
    public async Task CallerOutsideAKilledTransportProcess_StillTimesOutOrCancels()
    {
        // The test body runs on the client process, the transport belongs to process "c". Killing "c" must not leave the
        // body's calls pending forever: their timers and cancellation tokens belong to the (live) caller.
        var s = new SimScheduler(1);
        var net = new SimNetwork(s);
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", NeverReplies());
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            using var cts = new CancellationTokenSource();
            Task<ReadOnlyMemory<byte>> timed = connection.CallAsync(Bytes("timed"), Second);
            Task<ReadOnlyMemory<byte>> cancellable = connection.CallAsync(Bytes("cancellable"), Timeout.InfiniteTimeSpan, cts.Token);
            await s.DelayAsync(10 * Ms);

            s.FindProcess("c")!.Kill();
            Assert.False(connection.IsOpen);
            cts.Cancel();
            Assert.True(cancellable.IsCanceled);
            await Assert.ThrowsAsync<TimeoutException>(() => timed);
        });
    }

    [Fact]
    public async Task MessagesArrivingAtAKilledProcess_AreCountedAsDropped()
    {
        var s = new SimScheduler(1);
        var net = new SimNetwork(s, new SimNetworkOptions { ResetPeersOnProcessDeath = false });
        SimNodeLoop server = s.LoopFactory.Create("n1");
        DelegateHandler handler = DelegateHandler.Echo();
        await s.RunAsync(async () =>
        {
            await ListenAsync(net, "n1", handler);
            IRpcConnection connection = await net.Create("c", "").ConnectAsync("mem://n1");
            await connection.SendAsync(Bytes("alive"));
            await s.DelayAsync(10 * Ms);
            server.Stop();
            for (int i = 0; i < 3; i++)
                await connection.SendAsync(Bytes("to the dead"));
            await s.DelayAsync(Second);
        });

        Assert.Equal([("alive", false)], handler.Received);
        Assert.Equal(4, net.MessagesSent);
        Assert.Equal(1, net.MessagesDelivered);
        Assert.Equal(3, net.MessagesDropped);
    }

    [Fact]
    public void RunAsync_FromInsideSimulatedWork_FailsWithoutRunningTheBody()
    {
        var s = new SimScheduler(1);
        bool bodyRan = false;
        Task? nested = null;
        s.Post(() => nested = s.RunAsync(() =>
        {
            bodyRan = true;
            return Task.CompletedTask;
        }));
        s.RunUntilIdle();

        Assert.IsType<InvalidOperationException>(nested!.Exception!.InnerException);
        Assert.False(bodyRan);
        Assert.Equal(0, s.ClientProcess.QueuedWorkCount);
    }

    [Fact]
    public void CrashRestartAndPartitionScenario_ReplaysExactly()
    {
        // HAR-001 beyond the ping workload: kills, restarts (new incarnations listening on the same endpoint), pauses,
        // per-node clock manipulation and a link policy decided by a seeded stream, all in one replayed scenario.
        static SimRunRecord Scenario(ulong seed)
        {
            var s = new SimScheduler(seed, new SimSchedulerOptions { RecordSteps = true });
            IRandomSource chaos = s.ForkRandom("chaos");
            var policy = new DelegateLinkPolicy
            {
                OnDecide = (_, _, _, _) => chaos.Next(10) switch
                {
                    0 => LinkDecision.Drop,
                    1 => LinkDecision.Duplicate,
                    2 => LinkDecision.Delayed(TimeSpan.FromTicks(chaos.Next(50_000))),
                    _ => LinkDecision.Deliver,
                },
            };
            var net = new SimNetwork(s, new SimNetworkOptions { MaxLatency = 3 * Ms }, policy);
            string[] names = ["n0", "n1", "n2"];
            var clocks = names.ToDictionary(n => n, n => s.CreateClock(n), StringComparer.Ordinal);
            var served = new Dictionary<string, int>(StringComparer.Ordinal);

            void Boot(string name)
            {
                SimNodeLoop loop = s.LoopFactory.Create(name);
                IRpcTransport transport = net.Create(name, "mem://" + name);
                loop.Post(async () =>
                {
                    await transport.StartAsync(new DelegateHandler((_, request, expectsReply, _) =>
                    {
                        served[name] = served.GetValueOrDefault(name) + 1;
                        return new ValueTask<ReadOnlyMemory<byte>?>(expectsReply ? Bytes(loop.Process.Id + ":" + SimText.Str(request)) : null);
                    }));
                    IRandomSource random = s.ForkRandom("node/" + loop.Process.Id);
                    for (int round = 0; round < 20; round++)
                    {
                        await Task.Delay(TimeSpan.FromTicks(10_000 + random.Next(40_000)), clocks[name]);
                        string peer = names[random.Next(names.Length)];
                        try
                        {
                            IRpcConnection connection = await transport.ConnectAsync("mem://" + peer);
                            ReadOnlyMemory<byte> reply = await connection.CallAsync(Bytes(name + "/" + round), 20 * Ms);
                            s.AddTrace($"{loop.Process.Id} got {SimText.Str(reply)}");
                            await connection.DisposeAsync();
                        }
                        catch (Exception ex) when (ex is TimeoutException or RpcConnectionException)
                        {
                            s.AddTrace($"{loop.Process.Id} -> {peer}: {ex.GetType().Name}");
                        }
                    }
                });
            }

            foreach (string name in names)
                Boot(name);
            s.RunFor(30 * Ms);
            s.FindProcess("n1")!.Kill();
            clocks["n2"].SetRate(1.5);
            s.FindProcess("n0")!.Pause();
            s.RunFor(20 * Ms);
            Boot("n1");
            clocks["n0"].Freeze();
            s.FindProcess("n0")!.Resume();
            s.RunFor(20 * Ms);
            clocks["n0"].Advance(15 * Ms);
            clocks["n0"].Unfreeze();
            s.RunUntilIdle(TimeSpan.FromMinutes(1));

            string digest = string.Join(",", served.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => p.Key + "=" + p.Value)) +
                $"|sent={net.MessagesSent},dropped={net.MessagesDropped},late={net.LateReplies},opened={net.ConnectionsOpened}";
            Assert.Empty(s.Violations);
            return SimRunRecord.Capture(s, digest);
        }

        SimRunRecord record = SimDeterminism.AssertReplayIsExact(Scenario, 42, runs: 3);
        Assert.Contains(record.Trace, line => line.Contains("process n1#2 created", StringComparison.Ordinal));
        SimDeterminism.AssertSeedsDiverge(Scenario, 1, 2, 3);
    }

    [Fact]
    public void TestClockOverTheSimulationClock_ReplaysExactly_UnderFreezeAndRateChanges()
    {
        // TestNode.Clock is a TestClock; layered over a node's SimTimeProvider its rescheduling must stay deterministic.
        static SimRunRecord Scenario(ulong seed)
        {
            var s = new SimScheduler(seed, new SimSchedulerOptions { RecordSteps = true });
            var clock = new TestClock(s.CreateClock("n0"));
            SimNodeLoop loop = s.LoopFactory.Create("n0");
            for (int i = 0; i < 8; i++)
            {
                int index = i;
                loop.Post(() => clock.CreateTimer(_ => s.AddTrace("tick " + index), null, 5 * Ms, 5 * Ms));
            }

            s.RunFor(12 * Ms);
            clock.Freeze();
            s.RunFor(20 * Ms);
            clock.SetRate(2.0);
            clock.Unfreeze();
            s.RunFor(30 * Ms);
            return SimRunRecord.Capture(s);
        }

        SimDeterminism.AssertReplayIsExact(Scenario, 7, runs: 3);
    }
}
