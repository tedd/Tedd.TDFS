using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>Tests of <see cref="SimNodeLoop"/> / <see cref="SimNodeLoopFactory"/> (INodeLoop semantics under simulation).</summary>
public sealed class SimNodeLoopTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);

    [Fact]
    public void Factory_BindsLoopsToProcessIncarnations()
    {
        var s = new SimScheduler(1);
        var created = new List<SimNodeLoop>();
        s.LoopFactory.LoopCreated += created.Add;
        INodeLoopFactory factory = s.LoopFactory;
        INodeLoop first = factory.Create("n0");
        SimNodeLoop sim = Assert.IsType<SimNodeLoop>(first);
        Assert.Equal("n0", first.Name);
        Assert.Equal("n0#1", sim.Process.Id);
        Assert.Same(sim, sim.Process.Loop);
        Assert.Same(sim.Process.Context, first.Context);
        Assert.Same(s.LoopFactory, s.LoopFactory);
        Assert.Throws<InvalidOperationException>(() => s.LoopFactory.Create("n0"));
        Assert.Throws<ArgumentException>(() => s.LoopFactory.Create(""));

        first.Stop();
        Assert.True(first.IsStopped);
        SimNodeLoop second = s.LoopFactory.Create("n0");
        Assert.Equal("n0#2", second.Process.Id);
        Assert.Equal([sim, second], created);
    }

    [Fact]
    public void Post_RunsOnTheLoopWithItsContextInstalled()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        var seen = new List<(bool OnLoop, bool Context)>();
        loop.Post(() => seen.Add((loop.IsOnLoop, ReferenceEquals(SynchronizationContext.Current, loop.Context))));
        s.Post(() => seen.Add((loop.IsOnLoop, ReferenceEquals(SynchronizationContext.Current, loop.Context))));
        s.RunUntilIdle();
        Assert.Contains((true, true), seen);
        Assert.Contains((false, false), seen);
        Assert.False(loop.IsOnLoop);
        Assert.Throws<ArgumentNullException>(() => loop.Post(null!));
    }

    [Fact]
    public void AwaitsInLoopCode_ResumeOnTheLoop_InVirtualTime()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        SimTimeProvider clock = s.CreateClock("n0");
        var seen = new List<(TimeSpan Time, bool OnLoop)>();
        loop.Post(async () =>
        {
            seen.Add((s.Now, loop.IsOnLoop));
            await Task.Delay(5 * Ms, clock);
            seen.Add((s.Now, loop.IsOnLoop));
            await Task.Yield();
            seen.Add((s.Now, loop.IsOnLoop));
        });
        s.RunUntilIdle();
        Assert.Equal([(TimeSpan.Zero, true), (5 * Ms, true), (5 * Ms, true)], seen);
        Assert.Empty(s.Violations);
    }

    [Fact]
    public void Pause_HoldsLoopWorkAndTimers_ResumeFiresOverdueTimersOnce()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        SimTimeProvider clock = s.CreateClock("n0");
        var fired = new List<TimeSpan>();
        ITimer? timer = null;
        loop.Post(() => timer = clock.CreateTimer(_ => fired.Add(s.Now), null, Ms, Ms));
        s.RunFor(3 * Ms);
        Assert.Equal([1 * Ms, 2 * Ms, 3 * Ms], fired);

        loop.Pause();
        Assert.True(loop.IsPaused);
        bool ranWork = false;
        loop.Post(() => ranWork = true);
        s.RunFor(10 * Ms);
        Assert.Equal(3, fired.Count);
        Assert.False(ranWork);

        loop.Resume();
        s.RunFor(TimeSpan.Zero);
        Assert.True(ranWork);
        Assert.Equal(4, fired.Count); // the overdue firing runs once; missed periods are not replayed
        Assert.Equal(13 * Ms, fired[^1]);
        s.RunFor(2 * Ms);
        Assert.Equal([14 * Ms, 15 * Ms], fired.Skip(4));
        timer!.Dispose();
    }

    [Fact]
    public void Stop_DiscardsWork_KillsTimers_AndCompletes()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        SimTimeProvider clock = s.CreateClock("n0");
        int fired = 0;
        int ran = 0;
        loop.Post(() => clock.CreateTimer(_ => fired++, null, Ms, Ms));
        s.RunFor(2 * Ms);
        Assert.Equal(2, fired);

        loop.Pause();
        loop.Post(() => ran++);
        loop.Stop();
        loop.Post(() => ran++);
        s.RunFor(TimeSpan.FromSeconds(1));

        Assert.Equal(2, fired);
        Assert.Equal(0, ran);
        Assert.True(loop.IsStopped);
        Assert.False(loop.IsPaused);
        Assert.True(loop.Completion.IsCompletedSuccessfully);
        Assert.Equal(2, loop.Process.DiscardedWorkCount);
    }

    [Fact]
    public async Task InvokeAsync_ReturnsResultsOnTheLoop()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        SimTimeProvider clock = s.CreateClock("n0");
        (int Value, bool OnLoop, bool AsyncOnLoop, bool VoidRan) result = await s.RunAsync(async () =>
        {
            int value = await loop.InvokeAsync(() => 5);
            bool onLoop = await loop.InvokeAsync(() => loop.IsOnLoop);
            bool asyncOnLoop = await loop.InvokeAsync(async () =>
            {
                await Task.Delay(Ms, clock);
                return loop.IsOnLoop;
            });
            bool voidRan = false;
            await loop.InvokeAsync(() => { voidRan = true; });
            await loop.InvokeAsync(async () => await Task.Delay(Ms, clock));
            return (value, onLoop, asyncOnLoop, voidRan);
        });

        Assert.Equal((5, true, true, true), result);
        await Assert.ThrowsAsync<FormatException>(() => s.RunAsync(() => loop.InvokeAsync(() => throw new FormatException())));
        Assert.Equal(0, loop.PendingInvocationCount);
        Assert.Empty(s.UnhandledExceptions);
    }

    [Fact]
    public async Task InvokeAsync_FaultsWhenTheLoopIsKilled()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        var never = new TaskCompletionSource();
        Task inProgress = loop.InvokeAsync(async () => await never.Task);
        s.RunUntilIdle(); // started, now awaiting
        loop.Pause();
        Task<int> queued = loop.InvokeAsync(() => 1);
        Assert.Equal(2, loop.PendingInvocationCount);

        loop.Stop();

        Assert.IsType<NodeLoopStoppedException>(Assert.Single(queued.Exception!.InnerExceptions));
        Assert.IsType<NodeLoopStoppedException>(Assert.Single(inProgress.Exception!.InnerExceptions));
        await Assert.ThrowsAsync<NodeLoopStoppedException>(() => loop.InvokeAsync(() => 2));
    }

    [Fact]
    public void DrainAsync_RunsQueuedWorkAndContinuations_ThenStops()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        SimTimeProvider clock = s.CreateClock("n0");
        var log = new List<string>();
        loop.Pause();
        loop.Post(() => log.Add("a"));
        loop.Post(async () =>
        {
            log.Add("b1");
            await Task.Yield();
            log.Add("b2");
        });
        Task drain = loop.DrainAsync();
        Assert.False(drain.IsCompleted);
        s.RunUntilIdle();

        Assert.True(drain.IsCompletedSuccessfully);
        Assert.True(loop.IsStopped);
        Assert.Equal(["a", "b1", "b2"], log);
        Assert.True(loop.DrainAsync().IsCompletedSuccessfully);
        _ = clock;
    }

    [Fact]
    public void DrainAsync_OnAnEmptyLoop_StopsImmediately_AndFromTheLoop_AfterTheCurrentItem()
    {
        var s = new SimScheduler(1);
        SimNodeLoop idle = s.LoopFactory.Create("idle");
        Assert.True(idle.DrainAsync().IsCompletedSuccessfully);
        Assert.True(idle.IsStopped);

        SimNodeLoop self = s.LoopFactory.Create("self");
        Task? drain = null;
        bool finishedItem = false;
        self.Post(() =>
        {
            drain = self.DrainAsync();
            self.Post(() => finishedItem = true); // posted while draining: still runs
        });
        s.RunUntilIdle();
        Assert.True(finishedItem);
        Assert.True(drain!.IsCompletedSuccessfully);
        Assert.True(self.IsStopped);
    }

    [Fact]
    public void DrainAsync_Cancelled_StopsAbruptly()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        loop.Pause();
        bool ran = false;
        loop.Post(() => ran = true);
        using var cts = new CancellationTokenSource();
        loop.Pause();
        Task drain = loop.DrainAsync(cts.Token); // resumes; the work is queued but not yet run
        cts.Cancel();
        s.RunUntilIdle();
        Assert.True(drain.IsCanceled);
        Assert.False(ran);
        Assert.True(loop.IsStopped);
    }

    [Fact]
    public void UnhandledException_RaisesTheLoopEvent()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { ThrowOnUnhandledException = false });
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        var seen = new List<(INodeLoop, Exception)>();
        loop.UnhandledException += (l, e) => seen.Add((l, e));
        loop.UnhandledException += (_, _) => throw new InvalidOperationException("swallowed");
        var failure = new FormatException();
        loop.Post(() => throw failure);
        s.RunUntilIdle();
        (INodeLoop source, Exception error) = Assert.Single(seen);
        Assert.Same(loop, source);
        Assert.Same(failure, error);
        Assert.Single(s.UnhandledExceptions);
    }

    [Fact]
    public void LoopAndBackgroundWork_ReportTheirLaneThroughIsOnLoop()
    {
        var s = new SimScheduler(1);
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        SimTimeProvider clock = s.CreateClock("n0");
        bool timerOnLoop = true;
        bool timerOwnedByNode = false;
        clock.CreateTimer(_ =>
        {
            timerOnLoop = loop.IsOnLoop;
            timerOwnedByNode = ReferenceEquals(s.CurrentProcess, loop.Process) && SynchronizationContext.Current is null;
        }, null, Ms, Timeout.InfiniteTimeSpan);
        s.RunUntilIdle();
        Assert.False(timerOnLoop); // timer callbacks are background work of the process, not loop work
        Assert.True(timerOwnedByNode);
    }
}
