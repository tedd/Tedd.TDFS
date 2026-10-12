namespace Tedd.Tdfs.Server;

/// <summary>Counter snapshot of a node (monotonic since process start unless noted).</summary>
/// <param name="ObjectsStored">Objects currently stored.</param>
/// <param name="ObjectBytesStored">Plaintext bytes currently stored.</param>
/// <param name="ObjectBytesReceived">Object bytes received from uploads and repair.</param>
/// <param name="ObjectPuts">Object put requests handled.</param>
/// <param name="ReceiptsIssued">Durability receipts issued.</param>
/// <param name="ObjectReads">Object read requests served.</param>
/// <param name="ObjectBytesRead">Object bytes served.</param>
/// <param name="ChecksumFailures">Checksum verification failures detected.</param>
/// <param name="RaftEntriesAppended">Raft entries appended to local logs.</param>
/// <param name="RaftEntriesApplied">Raft entries applied to state machines.</param>
/// <param name="Elections">Elections started by this node.</param>
/// <param name="LeaseExpiries">Handle leases expired by this node as leader.</param>
/// <param name="TransactionConflicts">Transactions rejected for conflicts.</param>
/// <param name="GcDeletedObjects">Objects deleted by garbage collection.</param>
/// <param name="GcDeletedBytes">Bytes deleted by garbage collection.</param>
/// <param name="RepairBytes">Bytes copied by repair.</param>
/// <param name="BackupBytesSent">Bytes sent to backup targets.</param>
/// <param name="RpcRequestsHandled">RPC requests handled.</param>
/// <param name="RejectedStaleWrites">Publications rejected by fencing (stale writer generation or incarnation).</param>
public sealed record NodeCounters(long ObjectsStored, long ObjectBytesStored, long ObjectBytesReceived, long ObjectPuts,
    long ReceiptsIssued, long ObjectReads, long ObjectBytesRead, long ChecksumFailures, long RaftEntriesAppended,
    long RaftEntriesApplied, long Elections, long LeaseExpiries, long TransactionConflicts, long GcDeletedObjects,
    long GcDeletedBytes, long RepairBytes, long BackupBytesSent, long RpcRequestsHandled, long RejectedStaleWrites)
{
    /// <summary>All counters zero.</summary>
    public static NodeCounters Zero { get; } = new(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    /// <summary>Field-wise sum (cluster totals).</summary>
    public NodeCounters Add(NodeCounters other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return new NodeCounters(
            ObjectsStored + other.ObjectsStored,
            ObjectBytesStored + other.ObjectBytesStored,
            ObjectBytesReceived + other.ObjectBytesReceived,
            ObjectPuts + other.ObjectPuts,
            ReceiptsIssued + other.ReceiptsIssued,
            ObjectReads + other.ObjectReads,
            ObjectBytesRead + other.ObjectBytesRead,
            ChecksumFailures + other.ChecksumFailures,
            RaftEntriesAppended + other.RaftEntriesAppended,
            RaftEntriesApplied + other.RaftEntriesApplied,
            Elections + other.Elections,
            LeaseExpiries + other.LeaseExpiries,
            TransactionConflicts + other.TransactionConflicts,
            GcDeletedObjects + other.GcDeletedObjects,
            GcDeletedBytes + other.GcDeletedBytes,
            RepairBytes + other.RepairBytes,
            BackupBytesSent + other.BackupBytesSent,
            RpcRequestsHandled + other.RpcRequestsHandled,
            RejectedStaleWrites + other.RejectedStaleWrites);
    }
}
