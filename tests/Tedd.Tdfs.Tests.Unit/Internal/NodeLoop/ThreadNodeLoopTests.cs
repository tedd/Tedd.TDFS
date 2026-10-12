using System.Collections.Concurrent;
using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Tests.Unit.Internal.NodeLoop;

/// <summary>Tests of the production node loop (<see cref="ThreadNodeLoopFactory"/> / <c>ThreadNodeLoop</c>).</summary>
public sealed class ThreadNodeLoopTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static INodeLoop CreateLoop(string name = "n0", INodeLog? log = null) =>
        (log is null ? ThreadNodeLoopFactory.Instance : new ThreadNodeLoopFactory(log)).Create(name);

    private static Task<int> LoopThreadIdAsync(INodeLoop loop) => loop.InvokeAsync(() => Environment.CurrentManagedThreadId);

    [Fact]
    public void Factory_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentException>(() => ThreadNodeLoopFactory.Instance.Create(""));
        Assert.Throws<ArgumentNullException>(() => ThreadNodeLoopFactory.Instance.Create(null!));
        Assert.Throws<ArgumentNullException>(() => new ThreadNodeLoopFactory(null!));
        Assert.Same(NullNodeLog.Instance, ThreadNodeLoopFactory.Instance.Log);
    }

    [Fact]
    public async Task Post_RunsItemsFifoOnOneDedicatedNamedThread()
    {
        INodeLoop loop = CreateLoop("fifo");
        try
        {
            var order = new List<int>();
            var threads = new HashSet<int>();
            string? threadName = null;
            bool onLoopInside = true;
            for (int i = 0; i < 1000; i++)
            {
                int index = i;
                loop.Post(() =>
                {
                    order.Add(index);
                    threads.Add(Environment.CurrentManagedThreadId);
                    threadName = Thread.CurrentThread.Name;
                    onLoopInside &= loop.IsOnLoop;
                });
            }

            int loopThread = await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
            Assert.Equal(Enumerable.Range(0, 1000), order);
            Assert.Equal([loopThread], threads);
            Assert.NotEqual(Environment.CurrentManagedThreadId, loopThread);
            Assert.Equal("tdfs-loop:fifo", threadName);
            Assert.True(onLoopInside);
            Assert.False(loop.IsOnLoop);
            Assert.Equal("fifo", loop.Name);
            Assert.Throws<ArgumentNullException>(() => loop.Post(null!));
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task Context_IsInstalled_AndAwaitsResumeOnTheLoop()
    {
        INodeLoop loop = CreateLoop();
        try
        {
            int loopThread = await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
            var observations = await loop.InvokeAsync(async () =>
            {
                var seen = new List<(bool OnLoop, int Thread, bool Context)>();
                seen.Add((loop.IsOnLoop, Environment.CurrentManagedThreadId, ReferenceEquals(SynchronizationContext.Current, loop.Context)));
                await Task.Delay(5, Ct);
                seen.Add((loop.IsOnLoop, Environment.CurrentManagedThreadId, ReferenceEquals(SynchronizationContext.Current, loop.Context)));
                await Task.Yield();
                seen.Add((loop.IsOnLoop, Environment.CurrentManagedThreadId, ReferenceEquals(SynchronizationContext.Current, loop.Context)));
                return seen;
            }).WaitAsync(Timeout, Ct);

            Assert.All(observations, o =>
            {
                Assert.True(o.OnLoop);
                Assert.Equal(loopThread, o.Thread);
                Assert.True(o.Context);
            });
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task Pause_HoldsWork_AndResumeReleasesItInOrder()
    {
        INodeLoop loop = CreateLoop();
        try
        {
            await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
            loop.Pause();
            Assert.True(loop.IsPaused);
            var ran = new ConcurrentQueue<int>();
            for (int i = 0; i < 20; i++)
            {
                int index = i;
                loop.Post(() => ran.Enqueue(index));
            }

            await Task.Delay(100, Ct);
            Assert.Empty(ran);
            Assert.Equal(20, ((ThreadNodeLoop)loop).QueuedCount);

            loop.Resume();
            Assert.False(loop.IsPaused);
            await loop.InvokeAsync(() => { }).WaitAsync(Timeout, Ct);
            Assert.Equal(Enumerable.Range(0, 20), ran);
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task Stop_DiscardsQueuedAndLaterWork_AndCompletes()
    {
        INodeLoop loop = CreateLoop();
        await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
        loop.Pause();
        int ran = 0;
        for (int i = 0; i < 10; i++)
            loop.Post(() => Interlocked.Increment(ref ran));

        loop.Stop();
        Assert.True(loop.IsStopped);
        await loop.Completion.WaitAsync(Timeout, Ct);
        loop.Post(() => Interlocked.Increment(ref ran));
        loop.Resume();
        await Task.Delay(50, Ct);

        Assert.Equal(0, Volatile.Read(ref ran));
        Assert.Equal(11, ((ThreadNodeLoop)loop).DiscardedCount);
        Assert.False(loop.IsOnLoop);
        Assert.False(loop.IsPaused);
        loop.Stop(); // idempotent
    }

    [Fact]
    public async Task Stop_FromInsideWork_TakesEffectAfterTheCurrentItem()
    {
        INodeLoop loop = CreateLoop();
        bool finishedCurrent = false;
        bool ranNext = false;
        loop.Post(() =>
        {
            loop.Stop();
            Thread.Sleep(20);
            finishedCurrent = true;
        });
        loop.Post(() => ranNext = true);

        await loop.Completion.WaitAsync(Timeout, Ct);
        Assert.True(finishedCurrent);
        Assert.False(ranNext);
    }

    [Fact]
    public async Task DrainAsync_RunsQueuedWorkAndItsContinuations_ThenStops()
    {
        INodeLoop loop = CreateLoop();
        await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
        loop.Pause();
        var log = new ConcurrentQueue<string>();
        loop.Post(() => log.Enqueue("a"));
        loop.Post(async () =>
        {
            log.Enqueue("b1");
            await Task.Yield(); // posts the continuation back to the loop while draining
            log.Enqueue("b2");
        });
        loop.Post(() => log.Enqueue("c"));

        await loop.DrainAsync(Ct).WaitAsync(Timeout, Ct);

        Assert.True(loop.IsStopped);
        Assert.True(loop.Completion.IsCompleted);
        Assert.Equal(["a", "b1", "c", "b2"], log);
        loop.Post(() => log.Enqueue("late"));
        await Task.Delay(30, Ct);
        Assert.DoesNotContain("late", log);
        await loop.DrainAsync(Ct).WaitAsync(Timeout, Ct); // already stopped: completes
    }

    [Fact]
    public async Task DrainAsync_Cancelled_StopsAbruptly()
    {
        INodeLoop loop = CreateLoop();
        using var gate = new ManualResetEventSlim(false);
        int ranAfterGate = 0;
        loop.Post(() => gate.Wait(Timeout));
        for (int i = 0; i < 5; i++)
            loop.Post(() => Interlocked.Increment(ref ranAfterGate));

        using var cts = new CancellationTokenSource();
        Task drain = loop.DrainAsync(cts.Token);
        cts.Cancel();
        gate.Set();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain.WaitAsync(Timeout, Ct));
        Assert.True(loop.IsStopped);
        await loop.Completion.WaitAsync(Timeout, Ct);
        Assert.Equal(0, Volatile.Read(ref ranAfterGate));
    }

    [Fact]
    public async Task UnhandledException_IsReportedAndLogged_AndTheLoopKeepsRunning()
    {
        var log = new RecordingLog();
        INodeLoop loop = CreateLoop("boom", log);
        try
        {
            var reported = new ConcurrentQueue<(INodeLoop Loop, Exception Error)>();
            loop.UnhandledException += (l, e) => reported.Enqueue((l, e));
            loop.UnhandledException += (_, _) => throw new InvalidOperationException("a failing handler is swallowed");
            var failure = new InvalidOperationException("work failed");
            loop.Post(() => throw failure);
            bool ranAfter = await loop.InvokeAsync(() => true).WaitAsync(Timeout, Ct);

            Assert.True(ranAfter);
            (INodeLoop source, Exception error) = Assert.Single(reported);
            Assert.Same(loop, source);
            Assert.Same(failure, error);
            Assert.Equal(1, ((ThreadNodeLoop)loop).UnhandledExceptionCount);
            Assert.Contains(log.Entries, e => e.Level == NodeLogLevel.Error && e.Node == "boom" && e.Category == "loop" && e.Message.Contains("work failed", StringComparison.Ordinal));
            Assert.Contains(log.Entries, e => e.Message.Contains("handler threw", StringComparison.Ordinal));
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task InvokeAsync_ReturnsResults_AndPropagatesFailuresAndCancellation()
    {
        INodeLoop loop = CreateLoop();
        try
        {
            Assert.Equal(42, await loop.InvokeAsync(() => 42).WaitAsync(Timeout, Ct));
            await loop.InvokeAsync(() => { }).WaitAsync(Timeout, Ct);

            int asyncResult = await loop.InvokeAsync(async () =>
            {
                await Task.Delay(5, Ct);
                return loop.IsOnLoop ? 7 : -1;
            }).WaitAsync(Timeout, Ct);
            Assert.Equal(7, asyncResult);

            bool awaitedOnLoop = false;
            await loop.InvokeAsync(async () =>
            {
                await Task.Delay(5, Ct);
                awaitedOnLoop = loop.IsOnLoop;
            }).WaitAsync(Timeout, Ct);
            Assert.True(awaitedOnLoop);

            await Assert.ThrowsAsync<FormatException>(() => loop.InvokeAsync(() => throw new FormatException()));
            await Assert.ThrowsAsync<FormatException>(() => loop.InvokeAsync(new Func<int>(() => throw new FormatException())));
            await Assert.ThrowsAsync<FormatException>(() => loop.InvokeAsync(async () =>
            {
                await Task.Yield();
                throw new FormatException();
            }));
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            Task canceled = loop.InvokeAsync(() => cts.Token.ThrowIfCancellationRequested());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);
            Assert.True(canceled.IsCanceled);
            Assert.Throws<ArgumentNullException>(() => { _ = loop.InvokeAsync((Action)null!); });

            // Unrelated to the loop's own exception reporting.
            Assert.Equal(0, ((ThreadNodeLoop)loop).UnhandledExceptionCount);
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task InvokeAsync_FaultsWithNodeLoopStopped_WhenTheLoopStopsFirst()
    {
        INodeLoop loop = CreateLoop("stopper");
        var never = new TaskCompletionSource();
        Task inProgress = loop.InvokeAsync(async () => await never.Task);
        await loop.InvokeAsync(() => { }).WaitAsync(Timeout, Ct); // the async work has started and is awaiting
        loop.Pause();
        Task<int> queued = loop.InvokeAsync(() => 1);

        loop.Stop();

        NodeLoopStoppedException a = await Assert.ThrowsAsync<NodeLoopStoppedException>(() => queued.WaitAsync(Timeout, Ct));
        Assert.Equal("stopper", a.LoopName);
        await Assert.ThrowsAsync<NodeLoopStoppedException>(() => inProgress.WaitAsync(Timeout, Ct));
        await Assert.ThrowsAsync<NodeLoopStoppedException>(() => loop.InvokeAsync(() => 2));
    }

    [Fact]
    public async Task InvokeAsync_ContinuationsOfExternalAwaitersDoNotRunOnTheLoopThread()
    {
        INodeLoop loop = CreateLoop();
        try
        {
            int loopThread = await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
            loop.Pause(); // register the continuation before the invocation can complete
            Task<int> invocation = loop.InvokeAsync(() => 1);
            Task<int> continuationThread = invocation.ContinueWith(
                _ => Environment.CurrentManagedThreadId, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            loop.Resume();
            Assert.NotEqual(loopThread, await continuationThread.WaitAsync(Timeout, Ct));
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task Send_RunsSynchronouslyOnTheLoop_AndPropagatesExceptions()
    {
        INodeLoop loop = CreateLoop();
        try
        {
            int loopThread = await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
            int sentOn = 0;
            loop.Context.Send(_ => sentOn = Environment.CurrentManagedThreadId, null);
            Assert.Equal(loopThread, sentOn);

            Assert.Throws<FormatException>(() => loop.Context.Send(_ => throw new FormatException(), null));

            bool inline = await loop.InvokeAsync(() =>
            {
                bool ran = false;
                loop.Context.Send(_ => ran = true, null); // on the loop: inline, no deadlock
                return ran;
            }).WaitAsync(Timeout, Ct);
            Assert.True(inline);
            Assert.Same(loop.Context, loop.Context.CreateCopy());
        }
        finally
        {
            loop.Stop();
        }

        await loop.Completion.WaitAsync(Timeout, Ct);
        Assert.Throws<NodeLoopStoppedException>(() => loop.Context.Send(_ => { }, null));
    }

    [Fact]
    public async Task Send_WaitingWhenTheLoopStops_Throws()
    {
        INodeLoop loop = CreateLoop();
        await LoopThreadIdAsync(loop).WaitAsync(Timeout, Ct);
        loop.Pause();
        Task send = Task.Run(() => loop.Context.Send(_ => { }, null), Ct);
        await Task.Delay(50, Ct);
        loop.Stop();
        await Assert.ThrowsAsync<NodeLoopStoppedException>(() => send.WaitAsync(Timeout, Ct));
    }

    [Fact]
    public async Task ConcurrentProducers_EveryItemRunsExactlyOnce_InPerProducerOrder()
    {
        const int Producers = 8;
        const int PerProducer = 5000;
        INodeLoop loop = CreateLoop();
        try
        {
            var lastSeen = new int[Producers];
            Array.Fill(lastSeen, -1);
            int executed = 0;
            bool ordered = true;
            var threads = new Thread[Producers];
            for (int p = 0; p < Producers; p++)
            {
                int producer = p;
                threads[p] = new Thread(() =>
                {
                    for (int i = 0; i < PerProducer; i++)
                    {
                        int item = i;
                        loop.Post(() =>
                        {
                            // Runs on the single loop thread: no synchronization needed for loop-owned state.
                            ordered &= lastSeen[producer] == item - 1;
                            lastSeen[producer] = item;
                            executed++;
                        });
                    }
                });
            }

            foreach (Thread thread in threads)
                thread.Start();
            foreach (Thread thread in threads)
                thread.Join(Timeout);

            int total = await loop.InvokeAsync(() => executed).WaitAsync(Timeout, Ct);
            Assert.Equal(Producers * PerProducer, total);
            Assert.True(ordered);
            Assert.All(lastSeen, last => Assert.Equal(PerProducer - 1, last));
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task Loops_AreIndependent()
    {
        INodeLoop a = CreateLoop("a");
        INodeLoop b = CreateLoop("b");
        try
        {
            a.Pause();
            Assert.Equal(5, await b.InvokeAsync(() => 5).WaitAsync(Timeout, Ct));
            Assert.NotEqual(await LoopThreadIdAsync(b).WaitAsync(Timeout, Ct), ((ThreadNodeLoop)a).ThreadId);
            a.Resume();
            Assert.Equal(6, await a.InvokeAsync(() => 6).WaitAsync(Timeout, Ct));
        }
        finally
        {
            a.Stop();
            b.Stop();
        }
    }

    private sealed class RecordingLog : INodeLog
    {
        public ConcurrentQueue<(NodeLogLevel Level, string Node, string Category, string Message)> Entries { get; } = new();

        public void Write(NodeLogLevel level, string node, string category, string message) =>
            Entries.Enqueue((level, node, category, message));
    }
}
