using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>Tests of <see cref="SimScheduler"/>: ordering, virtual time, run APIs, failure reporting and HAR-002 detection.</summary>
public sealed class SimSchedulerTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);

    [Fact]
    public void Options_AreValidated()
    {
        Assert.Throws<ArgumentException>(() => new SimScheduler(1, new SimSchedulerOptions { MaxStepsPerInstant = 0 }));
        Assert.Throws<ArgumentException>(() => new SimScheduler(1, new SimSchedulerOptions { DefaultRunTimeout = TimeSpan.Zero }));
        Assert.Throws<ArgumentException>(() => new SimScheduler(1, new SimSchedulerOptions { ClientProcessName = " " }));
        Assert.Throws<ArgumentException>(() => new SimScheduler(1, new SimSchedulerOptions { StallGracePeriod = TimeSpan.FromSeconds(-1) }));
    }

    [Fact]
    public void FifoMode_RunsItemsInPostOrderAcrossLanes()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { Mode = SimScheduleMode.Fifo });
        SimProcess a = s.CreateProcess("a");
        SimProcess b = s.CreateProcess("b");
        var order = new List<string>();
        a.Post(() => order.Add("a1"));
        b.Post(() => order.Add("b1"));
        a.Post(() => order.Add("a2"));
        s.Post(() => order.Add("g1"));
        b.Post(() => order.Add("b2"));

        Assert.Equal(5, s.RunUntilIdle());
        Assert.Equal(["a1", "b1", "a2", "g1", "b2"], order);
        Assert.Equal(5, s.StepCount);
    }

    [Fact]
    public void RandomMode_KeepsEachLaneFifo_AndReplaysPerSeed()
    {
        static List<string> Run(ulong seed)
        {
            var s = new SimScheduler(seed);
            var order = new List<string>();
            for (int p = 0; p < 3; p++)
            {
                SimProcess process = s.CreateProcess("p" + p);
                for (int i = 0; i < 20; i++)
                {
                    string label = $"p{p}:{i:D2}";
                    process.Post(() => order.Add(label));
                }
            }

            s.RunUntilIdle();
            return order;
        }

        List<string> first = Run(7);
        Assert.Equal(first, Run(7));
        for (int p = 0; p < 3; p++)
        {
            List<string> lane = first.Where(x => x.StartsWith($"p{p}:", StringComparison.Ordinal)).ToList();
            Assert.Equal(lane.Order(StringComparer.Ordinal), lane);
            Assert.Equal(20, lane.Count);
        }

        int distinct = Enumerable.Range(1, 6).Select(seed => string.Join(",", Run((ulong)seed))).Distinct().Count();
        Assert.True(distinct > 1, "different seeds should explore different interleavings");
    }

    [Fact]
    public void VirtualTime_AdvancesOnlyWhenIdle_ToTheNextDueEntry()
    {
        var s = new SimScheduler(1);
        var times = new List<TimeSpan>();
        s.Schedule(10 * Ms, () => times.Add(s.Now));
        s.Schedule(5 * Ms, () => times.Add(s.Now));
        s.Post(() =>
        {
            times.Add(s.Now);
            s.Post(() => times.Add(s.Now)); // still the same instant
        });

        s.RunUntilIdle();
        Assert.Equal([TimeSpan.Zero, TimeSpan.Zero, 5 * Ms, 10 * Ms], times);
        Assert.Equal(10 * Ms, s.Now);
        Assert.Equal(s.Options.StartTimeUtc + 10 * Ms, s.UtcNow);
    }

    [Fact]
    public void Schedule_CanBeCancelled_AndValidatesArguments()
    {
        var s = new SimScheduler(1);
        bool ran = false;
        IDisposable handle = s.Schedule(Ms, () => ran = true);
        handle.Dispose();
        s.RunUntilIdle();
        Assert.False(ran);
        Assert.Equal(TimeSpan.Zero, s.Now);
        Assert.Throws<ArgumentOutOfRangeException>(() => s.Schedule(-Ms, () => { }));
        Assert.Throws<ArgumentNullException>(() => s.Schedule(Ms, null!));
        Assert.Throws<ArgumentException>(() => s.Schedule(Ms, () => { }, new SimScheduler(2).ClientProcess));
    }

    [Fact]
    public void RunUntil_StopsAsSoonAsTheConditionHolds()
    {
        var s = new SimScheduler(1);
        int counter = 0;
        for (int i = 1; i <= 3; i++)
            s.Schedule(i * Ms, () => counter++);

        s.RunUntil(() => counter == 2);
        Assert.Equal(2, counter);
        Assert.Equal(2 * Ms, s.Now);
        s.RunUntilIdle();
        Assert.Equal(3, counter);
    }

    [Fact]
    public void RunUntil_ThrowsWhenTheVirtualBudgetRunsOut()
    {
        var s = new SimScheduler(1);
        int ticks = 0;
        using ITimer timer = s.Clock.CreateTimer(_ => ticks++, null, Ms, Ms);

        SimulationTimeoutException ex = Assert.Throws<SimulationTimeoutException>(() => s.RunUntil(() => false, 50 * Ms, "never"));
        Assert.Equal(50 * Ms, ex.VirtualTime);
        Assert.Equal(50 * Ms, s.Now);
        Assert.Equal(50, ticks);
        Assert.Contains("never", ex.Message, StringComparison.Ordinal);
        Assert.Throws<SimulationTimeoutException>(() => s.RunUntilIdle(10 * Ms));
    }

    [Fact]
    public void RunUntil_ThrowsStalledWhenNothingCanRun()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { StallGracePeriod = TimeSpan.Zero });
        SimulationStalledException ex = Assert.Throws<SimulationStalledException>(() => s.RunUntil(() => false, because: "a miracle"));
        Assert.Contains("a miracle", ex.Message, StringComparison.Ordinal);
        Assert.Contains("processes:", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RunFor_AdvancesTheClockExactly()
    {
        var s = new SimScheduler(1);
        bool fired = false;
        s.Schedule(TimeSpan.FromSeconds(2), () => fired = true);
        s.RunFor(TimeSpan.FromSeconds(1));
        Assert.Equal(TimeSpan.FromSeconds(1), s.Now);
        Assert.False(fired);
        s.RunFor(TimeSpan.FromSeconds(1));
        Assert.True(fired);
        Assert.Equal(TimeSpan.FromSeconds(2), s.Now);
        s.RunFor(TimeSpan.FromSeconds(3));
        Assert.Equal(TimeSpan.FromSeconds(5), s.Now);
        Assert.Throws<ArgumentOutOfRangeException>(() => s.RunFor(-Ms));
    }

    [Fact]
    public void RunOne_RunsOneItem_AdvancingTimeWhenNeeded()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { Mode = SimScheduleMode.Fifo });
        var ran = new List<int>();
        s.Post(() => ran.Add(1));
        s.Post(() => ran.Add(2));
        s.Schedule(5 * Ms, () => ran.Add(3));

        Assert.True(s.RunOne());
        Assert.Equal([1], ran);
        Assert.True(s.RunOne());
        Assert.True(s.RunOne());
        Assert.Equal([1, 2, 3], ran);
        Assert.Equal(5 * Ms, s.Now);
        Assert.False(s.RunOne());
    }

    [Fact]
    public async Task RunAsync_RunsTheBodyOnTheClientContext_AndReturnsItsResult()
    {
        var s = new SimScheduler(1);
        int testThread = Environment.CurrentManagedThreadId;
        var observations = new List<(bool Context, bool Thread, bool Lane)>();
        int result = await s.RunAsync(async () =>
        {
            observations.Add((ReferenceEquals(SynchronizationContext.Current, s.ClientContext), Environment.CurrentManagedThreadId == testThread, s.ClientProcess.IsOnMainLane));
            await s.DelayAsync(TimeSpan.FromSeconds(3));
            observations.Add((ReferenceEquals(SynchronizationContext.Current, s.ClientContext), Environment.CurrentManagedThreadId == testThread, s.ClientProcess.IsOnMainLane));
            Assert.Equal(TimeSpan.FromSeconds(3), s.Now);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.All(observations, o => Assert.True(o.Context && o.Thread && o.Lane));
        Assert.Empty(s.Violations);
        s.Run(() => Task.CompletedTask);
        Assert.Equal(5, s.Run(() => Task.FromResult(5)));
    }

    [Fact]
    public async Task RunAsync_PropagatesTheBodyException()
    {
        var s = new SimScheduler(1);
        await Assert.ThrowsAsync<InvalidOperationException>(() => s.RunAsync(async () =>
        {
            await s.DelayAsync(Ms);
            throw new InvalidOperationException("boom");
        }));
        await Assert.ThrowsAsync<FormatException>(() => s.RunAsync<int>(() => throw new FormatException()));
        Assert.Throws<InvalidOperationException>(() => s.Run(() => null!));
        Assert.Empty(s.UnhandledExceptions);
    }

    [Fact]
    public async Task RunAsync_ReportsAStallInsteadOfHanging()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { StallGracePeriod = TimeSpan.Zero });
        var never = new TaskCompletionSource();
        await Assert.ThrowsAsync<SimulationStalledException>(() => s.RunAsync(() => never.Task));
    }

    [Fact]
    public void Run_CannotBeReentered()
    {
        var s = new SimScheduler(1);
        Assert.Throws<InvalidOperationException>(() => s.Run(() =>
        {
            s.RunUntilIdle();
            return Task.CompletedTask;
        }));
        Assert.Throws<InvalidOperationException>(() => s.RunUntil(() => s.RunOne()));
    }

    [Fact]
    public void UnhandledException_IsRecordedReportedAndFailsTheRun()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        var events = new List<SimUnhandledError>();
        s.UnhandledException += events.Add;
        var failure = new InvalidOperationException("work failed");
        p.Post(() => throw failure);

        SimulationException ex = Assert.Throws<SimulationException>(() => s.RunUntilIdle());
        Assert.Same(failure, ex.InnerException);
        SimUnhandledError error = Assert.Single(s.UnhandledExceptions);
        Assert.Equal("n0#1", error.Process);
        Assert.Equal("n0#1/main", error.Lane);
        Assert.Same(failure, error.Exception);
        Assert.Equal([error], events);
        Assert.Contains(s.Trace, line => line.Contains("UNHANDLED", StringComparison.Ordinal));
        s.RunUntilIdle(); // reported once
    }

    [Fact]
    public void UnhandledException_WithThrowDisabled_IsRecordedAndTheRunContinues()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { ThrowOnUnhandledException = false, Mode = SimScheduleMode.Fifo });
        SimProcess p = s.CreateProcess("n0");
        bool ranAfter = false;
        p.Post(() => throw new FormatException());
        p.Post(() => ranAfter = true);
        s.RunUntilIdle();
        Assert.True(ranAfter);
        Assert.IsType<FormatException>(Assert.Single(s.UnhandledExceptions).Exception);
    }

    [Fact]
    public async Task ForeignThreadPost_WhileDriving_IsAViolation()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        int foreignThread = 0;
        SimulationViolationException ex = await Assert.ThrowsAsync<SimulationViolationException>(() => s.RunAsync(async () =>
        {
            var thread = new Thread(() =>
            {
                foreignThread = Environment.CurrentManagedThreadId;
                p.Post(() => { });
            }) { Name = "intruder" };
            thread.Start();
            thread.Join();
            await s.DelayAsync(Ms);
        }));

        SimViolation violation = Assert.Single(s.Violations);
        Assert.Same(violation, ex.Violation);
        Assert.Equal(SimViolationKind.ForeignThread, violation.Kind);
        Assert.Equal(foreignThread, violation.ThreadId);
        Assert.Equal("intruder", violation.ThreadName);
        Assert.False(violation.IsThreadPoolThread);
        Assert.Contains("post work", violation.Operation, StringComparison.Ordinal);
        Assert.NotNull(violation.StackTrace);
    }

    [Fact]
    public void ForeignThreadPost_WithThrowDisabled_IsRecordedAndTheWorkStillRuns()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { ThrowOnViolation = false });
        SimProcess p = s.CreateProcess("n0");
        bool ran = false;
        s.Run(async () =>
        {
            var thread = new Thread(() => p.Post(() => ran = true));
            thread.Start();
            thread.Join();
            await s.DelayAsync(Ms);
        });

        Assert.True(ran);
        Assert.Single(s.Violations);
    }

    [Fact]
    public void RealTimerInSimulatedNodeCode_IsReportedAsAnUncontrolledDependency()
    {
        // HAR-002 style: node code awaits a real timer; its continuation comes back from the thread pool.
        var s = new SimScheduler(3, new SimSchedulerOptions { StallGracePeriod = TimeSpan.FromSeconds(5) });
        SimNodeLoop loop = s.LoopFactory.Create("n0");
        bool done = false;
        var fired = new TaskCompletionSource();
        using var timer = new Timer(_ => fired.SetResult(), null, Timeout.Infinite, Timeout.Infinite);
        loop.Post(async () =>
        {
            // Arm the real timer on the next loop turn, after the await has captured the node context.
            loop.Post(() => timer.Change(0, Timeout.Infinite));
            await fired.Task;
            done = true;
        });

        SimulationViolationException ex = Assert.Throws<SimulationViolationException>(() => s.RunUntil(() => done, TimeSpan.FromSeconds(1)));
        Assert.Equal(SimViolationKind.ForeignThread, ex.Violation.Kind);
        Assert.True(ex.Violation.IsThreadPoolThread);
        Assert.NotEmpty(s.Violations);
    }

    [Fact]
    public void CompletingOnAMainLane_SendsConfigureAwaitFalseContinuationsToTheThreadPool_UnlessCompletedWithoutContext()
    {
        // The runtime refuses to inline ConfigureAwait(false) continuations on a thread with a custom synchronization
        // context. Unwrapped completions on a main lane therefore escape the simulation (and are caught as violations);
        // CompleteWithoutContext keeps them inline.
        static (bool InlineOnSimThread, IReadOnlyList<SimViolation> Violations) Scenario(bool wrap)
        {
            var s = new SimScheduler(1, new SimSchedulerOptions { ThrowOnViolation = false, StallGracePeriod = TimeSpan.FromSeconds(5) });
            SimProcess p = s.CreateProcess("n0");
            var source = new TaskCompletionSource();
            bool inline = false;
            bool continued = false;
            async Task SdkStyleAsync()
            {
                await source.Task.ConfigureAwait(false);
                inline = s.IsOnSimulationThread;
                s.Post(() => continued = true); // touches the simulation from wherever it continued
            }

            _ = SdkStyleAsync();
            p.Post(() =>
            {
                if (wrap)
                    SimScheduler.CompleteWithoutContext(() => source.SetResult());
                else
                    source.SetResult();
            });
            s.RunUntil(() => continued, TimeSpan.FromSeconds(1));
            return (inline, s.Violations);
        }

        (bool wrappedInline, IReadOnlyList<SimViolation> wrappedViolations) = Scenario(wrap: true);
        Assert.True(wrappedInline);
        Assert.Empty(wrappedViolations);

        (bool rawInline, IReadOnlyList<SimViolation> rawViolations) = Scenario(wrap: false);
        Assert.False(rawInline);
        Assert.True(Assert.Single(rawViolations).IsThreadPoolThread);
    }

    [Fact]
    public void ForeignPostWhileIdle_AfterARun_IsAViolation()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        s.RunUntilIdle();
        var thread = new Thread(() => p.Post(() => { }));
        thread.Start();
        thread.Join();

        Assert.Single(s.Violations);
        Assert.Throws<SimulationViolationException>(() => s.RunUntilIdle());
    }

    [Fact]
    public void SetupFromAnotherThreadBeforeTheFirstRun_IsAllowed()
    {
        SimScheduler? s = null;
        SimProcess? p = null;
        bool ran = false;
        var thread = new Thread(() =>
        {
            s = new SimScheduler(1);
            p = s.CreateProcess("n0");
            p.Post(() => ran = true);
        });
        thread.Start();
        thread.Join();

        s!.RunUntilIdle();
        Assert.True(ran);
        Assert.Empty(s.Violations);
    }

    [Fact]
    public void ReportViolation_IsRecorded()
    {
        var s = new SimScheduler(1);
        s.ReportViolation("wall clock read directly");
        SimViolation violation = Assert.Single(s.Violations);
        Assert.Equal(SimViolationKind.Reported, violation.Kind);
        Assert.Throws<SimulationViolationException>(() => s.RunUntilIdle());
    }

    [Fact]
    public void Livelock_IsDetected()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { MaxStepsPerInstant = 1000 });
        SimProcess p = s.CreateProcess("spinner");
        void Spin() => p.Post(Spin);
        p.Post(Spin);
        SimulationException ex = Assert.Throws<SimulationException>(() => s.RunUntilIdle());
        Assert.Contains("Livelock", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Trace_RecordsEventsAtVirtualTime_AndOptionallySteps()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { RecordSteps = true });
        s.AddTrace("hello");
        s.Schedule(5 * Ms, () => s.AddTrace("later"), description: "the later item");
        s.RunUntilIdle();

        IReadOnlyList<string> trace = s.Trace;
        Assert.Contains("[0.0000000s] hello", trace);
        Assert.Contains("[0.0050000s] later", trace);
        Assert.Contains(trace, line => line.StartsWith("[0.0050000s] step #", StringComparison.Ordinal) && line.EndsWith(": the later item", StringComparison.Ordinal));
        Assert.Contains("[0.0000000s] process client#1 created", trace);
        Assert.Equal("1.2345678s", SimScheduler.FormatTime(TimeSpan.FromTicks(12_345_678)));
    }

    [Fact]
    public void Trace_StopsAtItsCapacity()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { MaxTraceEntries = 3 });
        for (int i = 0; i < 10; i++)
            s.AddTrace("event " + i);
        Assert.Equal(3, s.Trace.Count);
        Assert.True(s.TraceTruncated);
    }

    [Fact]
    public void Processes_HaveIncarnations()
    {
        var s = new SimScheduler(1);
        SimProcess first = s.CreateProcess("n0");
        Assert.Equal("n0#1", first.Id);
        Assert.Equal(1, first.Incarnation);
        Assert.Throws<InvalidOperationException>(() => s.CreateProcess("n0"));
        Assert.Same(first, s.GetOrCreateProcess("n0"));
        Assert.Same(first, s.FindProcess("n0"));

        first.Kill();
        Assert.Null(s.FindProcess("n0"));
        SimProcess second = s.GetOrCreateProcess("n0");
        Assert.Equal("n0#2", second.Id);
        Assert.Equal([s.ClientProcess, first, second], s.Processes);
        Assert.Equal("client#1", s.ClientProcess.Id);
        Assert.Throws<ArgumentException>(() => s.CreateProcess(" "));
        Assert.Throws<ArgumentException>(() => s.CreateProcess("bad\nname"));
    }

    [Fact]
    public void Pause_HoldsEveryLaneOfTheProcess_ResumeReleasesThem()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        var ran = new List<string>();
        p.Pause();
        Assert.True(p.IsPaused);
        p.Post(() => ran.Add("main"));
        s.Schedule(Ms, () => ran.Add("scheduled"), p);
        s.Post(() => ran.Add("global"));

        s.RunUntilIdle(); // held work is not runnable: the simulation is idle
        Assert.Equal(["global"], ran);
        Assert.Equal(2, p.QueuedWorkCount);

        p.Resume();
        s.RunUntilIdle();
        Assert.Equal(3, ran.Count);
        Assert.Equal(0, p.QueuedWorkCount);
        Assert.Contains(s.Trace, line => line.EndsWith("process n0#1 paused", StringComparison.Ordinal));
    }

    [Fact]
    public void Kill_DiscardsQueuedAndLaterWork()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        var killed = new List<SimProcess>();
        s.ProcessKilled += killed.Add;
        int ran = 0;
        for (int i = 0; i < 3; i++)
            p.Post(() => ran++);
        s.Schedule(Ms, () => ran++, p);

        p.Kill();
        p.Post(() => ran++);
        s.RunUntilIdle();

        Assert.Equal(0, ran);
        Assert.True(p.IsKilled);
        Assert.True(p.Exited.IsCompletedSuccessfully);
        Assert.Equal(5, p.DiscardedWorkCount);
        Assert.Equal([p], killed);
        p.Kill(); // idempotent
        p.Resume();
        Assert.True(p.IsKilled);
    }

    [Fact]
    public void Crash_FromTheProcessesOwnWork_UnwindsWithoutAnError()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        bool continuedAfterCrash = false;
        p.Post(() =>
        {
            p.Crash();
            continuedAfterCrash = true;
        });

        s.RunUntilIdle();
        Assert.False(continuedAfterCrash);
        Assert.True(p.IsKilled);
        Assert.Empty(s.UnhandledExceptions);

        // Crashing from elsewhere does not throw.
        SimProcess q = s.CreateProcess("n1");
        q.Crash();
        Assert.True(q.IsKilled);
    }

    [Fact]
    public void ExceptionFromAKilledProcess_IsIgnored()
    {
        var s = new SimScheduler(1);
        SimProcess p = s.CreateProcess("n0");
        p.Post(() =>
        {
            p.Kill();
            throw new InvalidOperationException("zombie noise");
        });
        s.RunUntilIdle();
        Assert.Empty(s.UnhandledExceptions);
    }

    [Fact]
    public void SynchronizationContextSend_RunsInlineAsTheTargetProcess()
    {
        var s = new SimScheduler(1);
        SimProcess target = s.CreateProcess("n0");
        (bool OnTarget, bool Context) inside = default;
        bool back = false;
        s.Post(() =>
        {
            target.Context.Send(_ => inside = (target.IsOnMainLane, ReferenceEquals(SynchronizationContext.Current, target.Context)), null);
            back = s.CurrentProcess is null && SynchronizationContext.Current is null;
        });
        s.RunUntilIdle();
        Assert.True(inside.OnTarget);
        Assert.True(inside.Context);
        Assert.True(back);

        target.Pause();
        s.Post(() => Assert.Throws<InvalidOperationException>(() => target.Context.Send(_ => { }, null)));
        s.RunUntilIdle();
        target.Kill();
        Assert.Throws<InvalidOperationException>(() => target.Context.Send(_ => { }, null));
    }

    [Fact]
    public void ScheduleFingerprint_IdentifiesTheInterleaving()
    {
        static ulong Run(ulong seed, SimScheduleMode mode)
        {
            var s = new SimScheduler(seed, new SimSchedulerOptions { Mode = mode });
            for (int p = 0; p < 4; p++)
            {
                SimProcess process = s.CreateProcess("p" + p);
                for (int i = 0; i < 10; i++)
                    process.Post(() => { });
            }

            s.RunUntilIdle();
            return s.ScheduleFingerprint;
        }

        Assert.Equal(Run(11, SimScheduleMode.Random), Run(11, SimScheduleMode.Random));
        Assert.NotEqual(Run(11, SimScheduleMode.Random), Run(12, SimScheduleMode.Random));
        Assert.Equal(Run(11, SimScheduleMode.Fifo), Run(12, SimScheduleMode.Fifo)); // FIFO ignores the seed
    }

    [Fact]
    public void AmbientExecutionContext_DoesNotLeakBetweenItems()
    {
        var s = new SimScheduler(1, new SimSchedulerOptions { Mode = SimScheduleMode.Fifo });
        var ambient = new AsyncLocal<string?>();
        string? seenBySecond = "unset";
        s.Post(() => ambient.Value = "leak");
        s.Post(() => seenBySecond = ambient.Value);
        s.RunUntilIdle();
        Assert.Null(seenBySecond);
    }

    [Fact]
    public void DescribeState_ListsLanesProcessesAndTimers()
    {
        var s = new SimScheduler(5);
        SimProcess p = s.CreateProcess("n0");
        p.Pause();
        p.Post(() => { });
        s.Schedule(Ms, () => { });
        string state = s.DescribeState();
        Assert.Contains("seed 5", state, StringComparison.Ordinal);
        Assert.Contains("n0#1 Paused queued=1", state, StringComparison.Ordinal);
        Assert.Contains("pending timed entries: 1", state, StringComparison.Ordinal);
        Assert.Equal(1, s.PendingTimedCount);
        Assert.Equal(0, s.RunnableLaneCount);
    }
}
