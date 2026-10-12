using System.Collections.Concurrent;
using Tedd.Tdfs.Transport.Internal;

namespace Tedd.Tdfs.Tests.Unit.Internal.Transport;

/// <summary>The thread-pool serial executor behind in-order inbound delivery.</summary>
public sealed class SerialExecutorTests
{
    [ThreadStatic] private static bool t_insidePost;

    [Fact]
    public async Task Items_RunInPostingOrder_OneAtATime_OffThePostersStack()
    {
        var seen = new ConcurrentQueue<int>();
        int running = 0;
        int overlaps = 0;
        int sameThread = 0;
        const int count = 5000; // more than one batch
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = new SerialExecutor<int>(item =>
        {
            if (Interlocked.Increment(ref running) != 1)
                Interlocked.Increment(ref overlaps);
            // Reentrancy check: the item must never run inline inside Post on the posting thread's stack.
            // (Thread ids are not used: the poster may legitimately return to the pool and run later items.)
            if (t_insidePost)
                Interlocked.Increment(ref sameThread);
            seen.Enqueue(item);
            Interlocked.Decrement(ref running);
            if (item == count - 1)
                done.TrySetResult();
        }, _ => { });

        t_insidePost = true;
        try
        {
            for (int i = 0; i < count; i++)
                executor.Post(i);
        }
        finally
        {
            t_insidePost = false;
        }
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(0, count), seen);
        Assert.Equal(0, overlaps);
        Assert.Equal(0, sameThread);
    }

    [Fact]
    public async Task ProcessingFaults_AreReported_AndDoNotStopTheExecutor()
    {
        var faults = new ConcurrentQueue<Exception>();
        var processed = new ConcurrentQueue<int>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = new SerialExecutor<int>(item =>
        {
            if (item % 2 == 0)
                throw new InvalidOperationException("item " + item);
            processed.Enqueue(item);
            if (item == 9)
                done.TrySetResult();
        }, faults.Enqueue);

        for (int i = 0; i < 10; i++)
            executor.Post(i);
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal([1, 3, 5, 7, 9], processed);
        Assert.Equal(5, faults.Count);
    }

    [Fact]
    public async Task ConcurrentPosters_LoseNothing()
    {
        var seen = new ConcurrentDictionary<int, byte>();
        const int posters = 8;
        const int perPoster = 2000;
        var executor = new SerialExecutor<int>(item => seen[item] = 0, _ => { });

        await Task.WhenAll(Enumerable.Range(0, posters).Select(p => Task.Run(() =>
        {
            for (int i = 0; i < perPoster; i++)
                executor.Post(p * perPoster + i);
        })));

        DateTime deadline = DateTime.UtcNow.AddSeconds(10);
        while (seen.Count < posters * perPoster && DateTime.UtcNow < deadline)
            await Task.Delay(5, TestContext.Current.CancellationToken);
        Assert.Equal(posters * perPoster, seen.Count);
    }
}
