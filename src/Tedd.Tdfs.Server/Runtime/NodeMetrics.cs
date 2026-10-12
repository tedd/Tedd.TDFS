using System.Diagnostics.Metrics;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Monotonic node counters (ARCHITECTURE §14) behind <see cref="NodeInspector.GetCountersAsync"/> and the
/// <c>Tedd.Tdfs</c> meter. Increments happen on the node loop; reads may happen anywhere (interlocked).
/// </summary>
internal sealed class NodeMetrics : IDisposable
{
    /// <summary>Name of the <see cref="System.Diagnostics.Metrics.Meter"/>.</summary>
    public const string MeterName = "Tedd.Tdfs";

    private readonly Meter _meter;
    private long _objectBytesReceived;
    private long _objectPuts;
    private long _receiptsIssued;
    private long _objectReads;
    private long _objectBytesRead;
    private long _checksumFailures;
    private long _raftEntriesAppended;
    private long _raftEntriesApplied;
    private long _elections;
    private long _leaseExpiries;
    private long _transactionConflicts;
    private long _gcDeletedObjects;
    private long _gcDeletedBytes;
    private long _repairBytes;
    private long _backupBytesSent;
    private long _rpcRequestsHandled;
    private long _rejectedStaleWrites;
    private long _objectsStored;
    private long _objectBytesStored;

    /// <summary>Creates the counters and registers observable instruments tagged with the node name.</summary>
    public NodeMetrics(string nodeName)
    {
        _meter = new Meter(MeterName);
        var tag = new KeyValuePair<string, object?>("node", nodeName);
        _meter.CreateObservableCounter("tdfs.objects.puts", () => new Measurement<long>(Interlocked.Read(ref _objectPuts), tag));
        _meter.CreateObservableCounter("tdfs.objects.bytes_received", () => new Measurement<long>(Interlocked.Read(ref _objectBytesReceived), tag));
        _meter.CreateObservableCounter("tdfs.objects.receipts", () => new Measurement<long>(Interlocked.Read(ref _receiptsIssued), tag));
        _meter.CreateObservableCounter("tdfs.objects.reads", () => new Measurement<long>(Interlocked.Read(ref _objectReads), tag));
        _meter.CreateObservableCounter("tdfs.objects.bytes_read", () => new Measurement<long>(Interlocked.Read(ref _objectBytesRead), tag));
        _meter.CreateObservableCounter("tdfs.objects.checksum_failures", () => new Measurement<long>(Interlocked.Read(ref _checksumFailures), tag));
        _meter.CreateObservableCounter("tdfs.raft.entries_appended", () => new Measurement<long>(Interlocked.Read(ref _raftEntriesAppended), tag));
        _meter.CreateObservableCounter("tdfs.raft.entries_applied", () => new Measurement<long>(Interlocked.Read(ref _raftEntriesApplied), tag));
        _meter.CreateObservableCounter("tdfs.raft.elections", () => new Measurement<long>(Interlocked.Read(ref _elections), tag));
        _meter.CreateObservableCounter("tdfs.rpc.requests", () => new Measurement<long>(Interlocked.Read(ref _rpcRequestsHandled), tag));
        _meter.CreateObservableGauge("tdfs.objects.stored", () => new Measurement<long>(Interlocked.Read(ref _objectsStored), tag));
        _meter.CreateObservableGauge("tdfs.objects.bytes_stored", () => new Measurement<long>(Interlocked.Read(ref _objectBytesStored), tag));
    }

    /// <summary>Records an object put and its received bytes.</summary>
    public void ObjectPut(long bytes)
    {
        Interlocked.Increment(ref _objectPuts);
        Interlocked.Add(ref _objectBytesReceived, bytes);
    }

    /// <summary>Records an issued receipt.</summary>
    public void ReceiptIssued() => Interlocked.Increment(ref _receiptsIssued);

    /// <summary>Records a served object read.</summary>
    public void ObjectRead(long bytes)
    {
        Interlocked.Increment(ref _objectReads);
        Interlocked.Add(ref _objectBytesRead, bytes);
    }

    /// <summary>Records a checksum verification failure.</summary>
    public void ChecksumFailure() => Interlocked.Increment(ref _checksumFailures);

    /// <summary>Records entries durably appended to a Raft log.</summary>
    public void EntriesAppended(int count) => Interlocked.Add(ref _raftEntriesAppended, count);

    /// <summary>Records entries applied to a state machine.</summary>
    public void EntriesApplied(int count) => Interlocked.Add(ref _raftEntriesApplied, count);

    /// <summary>Records an election started by this node.</summary>
    public void ElectionStarted() => Interlocked.Increment(ref _elections);

    /// <summary>Records an expired handle lease (volume service).</summary>
    public void LeaseExpired(int count = 1) => Interlocked.Add(ref _leaseExpiries, count);

    /// <summary>Records a transaction conflict (volume service).</summary>
    public void TransactionConflict() => Interlocked.Increment(ref _transactionConflicts);

    /// <summary>Records objects deleted by garbage collection.</summary>
    public void GcDeleted(long objects, long bytes)
    {
        Interlocked.Add(ref _gcDeletedObjects, objects);
        Interlocked.Add(ref _gcDeletedBytes, bytes);
    }

    /// <summary>Records bytes copied by repair.</summary>
    public void RepairCopied(long bytes) => Interlocked.Add(ref _repairBytes, bytes);

    /// <summary>Records bytes sent to backup targets.</summary>
    public void BackupSent(long bytes) => Interlocked.Add(ref _backupBytesSent, bytes);

    /// <summary>Records a handled RPC request.</summary>
    public void RpcHandled() => Interlocked.Increment(ref _rpcRequestsHandled);

    /// <summary>Records a publication rejected by fencing.</summary>
    public void RejectedStaleWrite() => Interlocked.Increment(ref _rejectedStaleWrites);

    /// <summary>Updates the stored-object gauges.</summary>
    public void SetStored(long objects, long bytes)
    {
        Interlocked.Exchange(ref _objectsStored, objects);
        Interlocked.Exchange(ref _objectBytesStored, bytes);
    }

    /// <summary>Snapshot of every counter.</summary>
    public NodeCounters Snapshot() => new(
        Interlocked.Read(ref _objectsStored),
        Interlocked.Read(ref _objectBytesStored),
        Interlocked.Read(ref _objectBytesReceived),
        Interlocked.Read(ref _objectPuts),
        Interlocked.Read(ref _receiptsIssued),
        Interlocked.Read(ref _objectReads),
        Interlocked.Read(ref _objectBytesRead),
        Interlocked.Read(ref _checksumFailures),
        Interlocked.Read(ref _raftEntriesAppended),
        Interlocked.Read(ref _raftEntriesApplied),
        Interlocked.Read(ref _elections),
        Interlocked.Read(ref _leaseExpiries),
        Interlocked.Read(ref _transactionConflicts),
        Interlocked.Read(ref _gcDeletedObjects),
        Interlocked.Read(ref _gcDeletedBytes),
        Interlocked.Read(ref _repairBytes),
        Interlocked.Read(ref _backupBytesSent),
        Interlocked.Read(ref _rpcRequestsHandled),
        Interlocked.Read(ref _rejectedStaleWrites));

    /// <summary>The meter (test use: listeners can tell this node's instruments apart).</summary>
    internal Meter Meter => _meter;

    /// <summary>Disposes the meter (thread-safe; also called by a crash, from outside the stopped loop).</summary>
    public void Dispose() => _meter.Dispose();
}
