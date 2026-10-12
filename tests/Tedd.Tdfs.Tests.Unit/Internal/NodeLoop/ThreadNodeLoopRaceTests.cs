using System.Collections.Concurrent;
using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Tests.Unit.Internal.NodeLoop;

/// <summary>
/// Adversarial tests of the production node loop: producers racing with Stop/DrainAsync, pausing while an item runs, and
/// failures escaping async-void work.
/// </summary>
public sealed class ThreadNodeLoopRaceTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task InvokeAsync_RacingWithStop_NeverLeavesATaskPending()
    {
        for (int iteration = 0; iteration < 20; iteration++)
        {
            INodeLoop loop = ThreadNodeLoopFactory.Instance.Create("race" + iteration);
            var tasks = new ConcurrentQueue<Task>();
            using var start = new ManualResetEventSlim(false);
            var producers = new Thread[4];
            for (int p = 0; p < producers.Length; p++)
            {
                int producer = p;
                producers[p] = new Thread(() =>
                {
                    start.Wait(Timeout);
                    for (int i = 0; i < 300; i++)
                    {
                        tasks.Enqueue(((i + producer) % 3) switch
                        {
                            0 => loop.InvokeAsync(() => { }),
                            1 => loop.InvokeAsync(() => i),
                            _ => loop.InvokeAsync(async () => await Task.Yield()),
                        });
                    }
                });
                producers[p].Start();
            }

            start.Set();
            Thread.SpinWait(2000 * (iteration % 5));
            loop.Stop();
            foreach (Thread producer in producers)
                Assert.True(producer.Join(Timeout));

            Task all = Task.WhenAll(tasks);
            await Task.WhenAny(all, Task.Delay(Timeout, Ct));
            Assert.True(all.IsCompleted, $"iteration {iteration}: an invocation never completed");
            Assert.All(tasks, t => Assert.True(
                t.IsCompletedSuccessfully || t.Exception?.InnerException is NodeLoopStoppedException,
                $"unexpected outcome {t.Status}: {t.Exception}"));
            await loop.Completion.WaitAsync(Timeout, Ct);
        }
    }

    [Fact]
    public async Task DrainAsync_RacingWithProducers_RunsAFifoPrefixOfEveryProducer()
    {
        for (int iteration = 0; iteration < 10; iteration++)
        {
            INodeLoop loop = ThreadNodeLoopFactory.Instance.Create("drain" + iteration);
            const int Producers = 4;
            const int PerProducer = 2000;
            var executed = new List<int>[Producers];
            for (int p = 0; p < Producers; p++)
                executed[p] = [];
            using var start = new ManualResetEventSlim(false);
            var producers = new Thread[Producers];
            for (int p = 0; p < Producers; p++)
            {
                int producer = p;
                producers[p] = new Thread(() =>
                {
                    start.Wait(Timeout);
                    for (int i = 0; i < PerProducer; i++)
                    {
                        int item = i;
                        loop.Post(() => executed[producer].Add(item)); // loop-owned state: only the loop thread touches it
                    }
                });
                producers[p].Start();
            }

            start.Set();
            Thread.SpinWait(5000 * (iteration % 4));
            await loop.DrainAsync(Ct).WaitAsync(Timeout, Ct);
            foreach (Thread producer in producers)
                Assert.True(producer.Join(Timeout));

            Assert.True(loop.IsStopped);
            for (int p = 0; p < Producers; p++)
                Assert.Equal(Enumerable.Range(0, executed[p].Count), executed[p]); // no gaps, no reordering
        }
    }

    [Fact]
    public async Task Pause_WhileAnItemRuns_TakesEffectBeforeTheNextItem()
    {
        INodeLoop loop = ThreadNodeLoopFactory.Instance.Create("pause-mid-item");
        try
        {
            using var entered = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            bool firstFinished = false;
            bool secondRan = false;
            loop.Post(() =>
            {
                entered.Set();
                release.Wait(Timeout);
                firstFinished = true;
            });
            loop.Post(() => secondRan = true);

            Assert.True(entered.Wait(Timeout));
            loop.Pause();
            release.Set();
            await Task.Delay(100, Ct);
            Assert.False(Volatile.Read(ref secondRan));

            loop.Resume();
            await loop.InvokeAsync(() => { }).WaitAsync(Timeout, Ct);
            Assert.True(firstFinished);
            Assert.True(secondRan);
        }
        finally
        {
            loop.Stop();
        }
    }

    [Fact]
    public async Task AsyncVoidWorkThatFails_IsReported_AndTheLoopKeepsRunning()
    {
        INodeLoop loop = ThreadNodeLoopFactory.Instance.Create("async-void");
        try
        {
            var reported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            loop.UnhandledException += (_, e) => reported.TrySetResult(e);
            var failure = new FormatException("after an await");
            loop.Post(async () =>
            {
                await Task.Yield();
                throw failure;
            });

            Assert.Same(failure, await reported.Task.WaitAsync(Timeout, Ct));
            Assert.True(await loop.InvokeAsync(() => loop.IsOnLoop).WaitAsync(Timeout, Ct));
        }
        finally
        {
            loop.Stop();
        }
    }
}
