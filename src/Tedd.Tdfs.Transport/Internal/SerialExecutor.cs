using System.Collections.Concurrent;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>
/// Runs posted items one at a time, in posting order, on the thread pool (never inline on the poster's stack). At most
/// one thread-pool work item is active per executor; a long burst is processed in batches so other work can interleave.
/// </summary>
/// <typeparam name="T">Item type.</typeparam>
internal sealed class SerialExecutor<T> : IThreadPoolWorkItem
{
    private const int MaxBatch = 256;

    private readonly ConcurrentQueue<T> _queue = new();
    private readonly Action<T> _process;
    private readonly Action<Exception> _onFault;
    private int _scheduled;

    /// <summary>Creates an executor.</summary>
    /// <param name="process">Processes one item (exceptions are passed to <paramref name="onFault"/>).</param>
    /// <param name="onFault">Receives exceptions thrown by <paramref name="process"/>; must not throw.</param>
    public SerialExecutor(Action<T> process, Action<Exception> onFault)
    {
        _process = process;
        _onFault = onFault;
    }

    /// <summary>Queues an item; it is processed after every item posted before it.</summary>
    public void Post(T item)
    {
        _queue.Enqueue(item);
        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
    }

    /// <inheritdoc />
    void IThreadPoolWorkItem.Execute()
    {
        while (true)
        {
            int processed = 0;
            while (processed < MaxBatch && _queue.TryDequeue(out T? item))
            {
                try
                {
                    _process(item);
                }
                catch (Exception ex)
                {
                    try
                    {
                        _onFault(ex);
                    }
                    catch (Exception)
                    {
                        // The fault reporter must never take the executor down.
                    }
                }

                processed++;
            }

            if (processed >= MaxBatch && !_queue.IsEmpty)
            {
                // Still scheduled: hand the thread back and continue in a fresh work item.
                ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                return;
            }

            Volatile.Write(ref _scheduled, 0);
            if (_queue.IsEmpty || Interlocked.CompareExchange(ref _scheduled, 1, 0) != 0)
                return;
        }
    }
}
