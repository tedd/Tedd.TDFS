using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>Tests of <see cref="SimTimeProvider"/>: global and per-node clocks, drift, freezing, jumps and timers (HAR-003).</summary>
public sealed class SimTimeProviderTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    [Fact]
    public void GlobalClock_FollowsVirtualTime()
    {
        var options = new SimSchedulerOptions { StartTimeUtc = new DateTimeOffset(2030, 5, 6, 7, 8, 9, TimeSpan.Zero), StartTimestamp = 12345 };
        var s = new SimScheduler(1, options);
        SimTimeProvider clock = s.Clock;
        Assert.True(clock.IsGlobal);
        Assert.Equal("global", clock.Name);
        Assert.Equal(TimeSpan.TicksPerSecond, clock.TimestampFrequency);
        Assert.Equal(options.StartTimeUtc, clock.GetUtcNow());
        Assert.Equal(12345, clock.GetTimestamp());
        Assert.Same(TimeZoneInfo.Utc, clock.LocalTimeZone);

        long before = clock.GetTimestamp();
        s.RunFor(TimeSpan.FromMilliseconds(1500));
        Assert.Equal(options.StartTimeUtc.AddMilliseconds(1500), clock.GetUtcNow());
        Assert.Equal(TimeSpan.FromMilliseconds(1500), clock.GetElapsedTime(before));
        Assert.Equal(TimeSpan.Zero, clock.WallOffset);
    }

    [Fact]
    public void GlobalClock_CannotBeManipulated()
    {
        var s = new SimScheduler(1);
        Assert.Throws<InvalidOperationException>(() => s.Clock.Advance(Second));
        Assert.Throws<InvalidOperationException>(() => s.Clock.JumpWall(Second));
        Assert.Throws<InvalidOperationException>(() => s.Clock.Freeze());
        Assert.Throws<InvalidOperationException>(() => s.Clock.Unfreeze());
        Assert.Throws<InvalidOperationException>(() => s.Clock.SetRate(2));
    }

    [Fact]
    public void CreateClock_ValidatesAndRegisters()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        Assert.Same(n0, s.FindClock("n0"));
        Assert.Same(n0, s.GetOrCreateClock("n0"));
        Assert.NotNull(s.GetOrCreateClock("n1"));
        Assert.Null(s.FindClock("missing"));
        Assert.Throws<InvalidOperationException>(() => s.CreateClock("n0"));
        Assert.Throws<InvalidOperationException>(() => s.CreateClock("global"));
        Assert.Throws<ArgumentException>(() => s.CreateClock("x", new SimClockOptions { Rate = 0 }));
        Assert.Throws<ArgumentException>(() => s.CreateClock("y", new SimClockOptions { Rate = double.NaN }));
        Assert.Equal("n0", n0.ProcessName);
        Assert.Null(s.CreateClock("unbound", new SimClockOptions { BindToProcess = false }).ProcessName);
        Assert.Throws<ArgumentOutOfRangeException>(() => n0.SetRate(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => n0.Advance(-Ms));
    }

    [Fact]
    public void NodeClock_HasAnIndependentWallOffsetAndMonotonicOrigin()
    {
        var s = new SimScheduler(1);
        s.RunFor(Second);
        SimTimeProvider n0 = s.CreateClock("n0", new SimClockOptions { WallOffset = TimeSpan.FromSeconds(-3), MonotonicOffset = TimeSpan.FromHours(5) });
        Assert.Equal(s.UtcNow - TimeSpan.FromSeconds(3), n0.GetUtcNow());
        Assert.Equal(TimeSpan.FromSeconds(-3), n0.WallOffset);
        Assert.Equal(s.Clock.GetTimestamp() + TimeSpan.FromHours(5).Ticks, n0.GetTimestamp());
        Assert.Equal(TimeSpan.Zero, n0.Elapsed);

        s.RunFor(2 * Second);
        Assert.Equal(2 * Second, n0.Elapsed);
        Assert.Equal(TimeSpan.FromSeconds(-3), n0.WallOffset);
    }

    [Fact]
    public void Rate_ScalesTheClockAndItsTimers()
    {
        var s = new SimScheduler(1);
        SimTimeProvider fast = s.CreateClock("fast", new SimClockOptions { Rate = 2.0 });
        SimTimeProvider slow = s.CreateClock("slow", new SimClockOptions { Rate = 0.5 });
        var fired = new List<(string, TimeSpan)>();
        fast.CreateTimer(_ => fired.Add(("fast", s.Now)), null, 2 * Second, Timeout.InfiniteTimeSpan);
        slow.CreateTimer(_ => fired.Add(("slow", s.Now)), null, 2 * Second, Timeout.InfiniteTimeSpan);

        s.RunFor(Second);
        Assert.Equal(2 * Second, fast.Elapsed);
        Assert.Equal(TimeSpan.FromMilliseconds(500), slow.Elapsed);
        s.RunUntilIdle();
        Assert.Equal([("fast", Second), ("slow", 4 * Second)], fired);

        fast.SetRate(1.0);
        s.RunFor(Second);
        Assert.Equal(TimeSpan.FromSeconds(9), fast.Elapsed); // 8 s during the first 4 s at rate 2, then 1 s at rate 1
        Assert.Equal(1.0, fast.Rate);
    }

    [Fact]
    public void Freeze_StopsTheClockAndItsTimers_UnfreezeResumesWithoutAJump()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        var fired = new List<TimeSpan>();
        n0.CreateTimer(_ => fired.Add(s.Now), null, 2 * Second, Timeout.InfiniteTimeSpan);
        s.RunFor(Second);
        n0.Freeze();
        Assert.True(n0.IsFrozen);
        long frozenAt = n0.GetTimestamp();
        DateTimeOffset frozenWall = n0.GetUtcNow();
        s.RunFor(10 * Second);
        Assert.Empty(fired);
        Assert.Equal(frozenAt, n0.GetTimestamp());
        Assert.Equal(frozenWall, n0.GetUtcNow());

        n0.Unfreeze();
        Assert.False(n0.IsFrozen);
        Assert.Equal(frozenAt, n0.GetTimestamp());
        s.RunUntilIdle();
        Assert.Equal([12 * Second], fired); // the remaining local second elapsed after unfreezing
        n0.Unfreeze(); // idempotent
    }

    [Fact]
    public void Advance_JumpsForwardAndFiresDueTimersAtTheCurrentInstant()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        var fired = new List<TimeSpan>();
        n0.CreateTimer(_ => fired.Add(s.Now), null, TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
        long before = n0.GetTimestamp();
        DateTimeOffset wallBefore = n0.GetUtcNow();

        n0.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(10), n0.GetElapsedTime(before));
        Assert.Equal(wallBefore.AddSeconds(10), n0.GetUtcNow());
        s.RunUntilIdle();
        Assert.Equal([TimeSpan.Zero], fired);
        Assert.Equal(TimeSpan.Zero, s.Now);
    }

    [Fact]
    public void JumpWall_MovesOnlyTheWallReading()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        var fired = new List<TimeSpan>();
        n0.CreateTimer(_ => fired.Add(s.Now), null, Second, Timeout.InfiniteTimeSpan);
        long mono = n0.GetTimestamp();
        DateTimeOffset wall = n0.GetUtcNow();

        n0.JumpWall(TimeSpan.FromHours(-1));
        Assert.Equal(wall.AddHours(-1), n0.GetUtcNow());
        Assert.Equal(mono, n0.GetTimestamp());
        n0.JumpWall(TimeSpan.FromMinutes(30));
        Assert.Equal(TimeSpan.FromMinutes(-30), n0.WallOffset);
        s.RunUntilIdle();
        Assert.Equal([Second], fired);
        Assert.Throws<ArgumentOutOfRangeException>(() => n0.JumpWall(TimeSpan.MaxValue / 2));
    }

    [Fact]
    public void NodeClocks_AreIndependent()
    {
        var s = new SimScheduler(1);
        SimTimeProvider a = s.CreateClock("a");
        SimTimeProvider b = s.CreateClock("b");
        var fired = new List<(string, TimeSpan)>();
        a.CreateTimer(_ => fired.Add(("a", s.Now)), null, Second, Timeout.InfiniteTimeSpan);
        b.CreateTimer(_ => fired.Add(("b", s.Now)), null, Second, Timeout.InfiniteTimeSpan);
        DateTimeOffset bWall = b.GetUtcNow();

        a.Freeze();
        a.JumpWall(TimeSpan.FromDays(1));
        s.RunFor(2 * Second);

        Assert.Equal([("b", Second)], fired);
        Assert.Equal(bWall.AddSeconds(2), b.GetUtcNow());
        Assert.Equal(2 * Second, b.Elapsed);
        Assert.Equal(TimeSpan.Zero, a.Elapsed);

        a.SetRate(4);
        a.Unfreeze();
        s.RunUntilIdle();
        Assert.Equal([("b", Second), ("a", TimeSpan.FromMilliseconds(2250))], fired);
    }

    [Fact]
    public void Timers_SupportOneShotPeriodicChangeAndDispose()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        var fired = new List<TimeSpan>();
        ITimer timer = n0.CreateTimer(_ => fired.Add(s.Now), null, Ms, 2 * Ms);
        Assert.Equal(1, n0.ActiveTimerCount);
        s.RunFor(5 * Ms);
        Assert.Equal([1 * Ms, 3 * Ms, 5 * Ms], fired);

        Assert.True(timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan));
        Assert.Equal(0, n0.ActiveTimerCount);
        s.RunFor(10 * Ms);
        Assert.Equal(3, fired.Count);

        Assert.True(timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan));
        s.RunUntilIdle();
        Assert.Equal(15 * Ms, fired[^1]);
        Assert.Equal(0, n0.ActiveTimerCount);

        timer.Change(Ms, Ms);
        timer.Dispose();
        Assert.False(timer.Change(Ms, Ms));
        s.RunFor(10 * Ms);
        Assert.Equal(4, fired.Count);
        Assert.Throws<ArgumentOutOfRangeException>(() => n0.CreateTimer(_ => { }, null, -Second, Timeout.InfiniteTimeSpan));
        Assert.Throws<ArgumentNullException>(() => n0.CreateTimer(null!, null, Second, Timeout.InfiniteTimeSpan));
    }

    [Fact]
    public void TimersDueAtTheSameInstant_FireInCreationOrderUnderFifo()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { Mode = SimScheduleMode.Fifo });
        var order = new List<int>();
        for (int i = 0; i < 5; i++)
        {
            int index = i;
            s.Clock.CreateTimer(_ => order.Add(index), null, Ms, Timeout.InfiniteTimeSpan);
        }

        s.RunUntilIdle();
        Assert.Equal([0, 1, 2, 3, 4], order);
    }

    [Fact]
    public async Task TaskDelay_AndCancellationTokenSource_UseVirtualTime()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0", new SimClockOptions { Rate = 2.0 });
        TimeSpan[] times = await s.RunAsync<TimeSpan[]>(async () =>
        {
            await Task.Delay(2 * Second, n0); // 1 s of global time at rate 2
            TimeSpan afterDelay = s.Now;
            using var cts = new CancellationTokenSource(2 * Second, n0);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task.Delay(TimeSpan.FromSeconds(100), n0, cts.Token));
            return [afterDelay, s.Now];
        });

        Assert.Equal([Second, 2 * Second], times);
        Assert.Empty(s.Violations);
    }

    [Fact]
    public async Task TestClock_LayeredOverTheSimulationClock_StaysInVirtualTime()
    {
        var s = new SimScheduler(1);
        var clock = new TestClock(s.Clock);
        TimeSpan[] times = await s.RunAsync<TimeSpan[]>(async () =>
        {
            await Task.Delay(Second, clock);
            TimeSpan first = s.Now;
            clock.SetRate(2.0);
            await Task.Delay(2 * Second, clock);
            return [first, s.Now];
        });

        Assert.Equal([Second, 2 * Second], times);
        Assert.Empty(s.Violations);
    }

    [Fact]
    public void Timestamps_NeverDecrease_UnderArbitraryClockManipulation()
    {
        var s = new SimScheduler(99);
        SimTimeProvider n0 = s.CreateClock("n0");
        IRandomSource random = s.ForkRandom("ops");
        long last = n0.GetTimestamp();
        for (int i = 0; i < 2000; i++)
        {
            switch (random.Next(6))
            {
                case 0: n0.Advance(TimeSpan.FromTicks(random.Next(1000))); break;
                case 1: n0.SetRate(0.25 + random.Next(16) / 4.0); break;
                case 2: n0.Freeze(); break;
                case 3: n0.Unfreeze(); break;
                case 4: n0.JumpWall(TimeSpan.FromTicks(random.Next(2001) - 1000)); break;
                default: s.RunFor(TimeSpan.FromTicks(random.Next(10_000))); break;
            }

            long now = n0.GetTimestamp();
            Assert.True(now >= last, $"timestamp decreased at step {i}");
            last = now;
        }
    }

    [Fact]
    public void TimersOfABoundClock_BelongToTheNodeProcess()
    {
        var s = new SimScheduler(1);
        SimTimeProvider n0 = s.CreateClock("n0");
        int fired = 0;
        n0.CreateTimer(_ => fired++, null, Ms, Ms); // created by the harness: still owned by process n0
        SimProcess process = Assert.IsType<SimProcess>(s.FindProcess("n0"));

        s.RunFor(2 * Ms);
        Assert.Equal(2, fired);
        process.Pause();
        s.RunFor(5 * Ms);
        Assert.Equal(2, fired);
        process.Kill();
        s.RunFor(5 * Ms);
        Assert.Equal(2, fired);
        Assert.Equal(0, n0.ActiveTimerCount);

        // An unbound clock's timers belong to whoever creates them (here: nobody, so they always run).
        SimTimeProvider unbound = s.CreateClock("free", new SimClockOptions { BindToProcess = false });
        int freeFired = 0;
        unbound.CreateTimer(_ => freeFired++, null, Ms, Timeout.InfiniteTimeSpan);
        s.RunUntilIdle();
        Assert.Equal(1, freeFired);
        Assert.Null(s.FindProcess("free"));
    }

    [Fact]
    public void TimerCallbacks_FlowTheExecutionContextOfTheirCreator()
    {
        var s = new SimScheduler(1);
        var ambient = new AsyncLocal<string?> { Value = "creator" };
        string? seen = null;
        s.Clock.CreateTimer(_ => seen = ambient.Value, null, Ms, Timeout.InfiniteTimeSpan);
        ambient.Value = null;
        s.RunUntilIdle();
        Assert.Equal("creator", seen);
    }
}
