using System.Buffers.Binary;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>How a directional link is cut.</summary>
internal enum LinkBlock
{
    /// <summary>Messages are dropped and connections over the link cannot be established.</summary>
    Cut,

    /// <summary>Messages silently vanish while existing connections stay open (half-open).</summary>
    BlackHole,
}

/// <summary>
/// The <see cref="ILinkPolicy"/> of a harness network fabric (ARCHITECTURE §10.4): every transport of the fabric (simulated,
/// in-memory or QUIC) consults it once per application message and per connection attempt with the network identities of
/// both ends. It implements partitions, isolation, directional cuts and black holes, mesh restrictions, drop/duplicate/delay
/// rules, realtime latency and bandwidth, and keeps the message log (<see cref="MessageLog"/>,
/// <see cref="TestNetwork.GetMessageAttempts"/>).
/// </summary>
/// <remarks>
/// Thread-safe; deterministic when called from one thread in a fixed order (the simulation). Correlation ids of logged
/// transmissions are harness sequence numbers: every request or one-way message observation gets a new one (a duplicated
/// message shares it between its copies); a reply is attributed to the oldest outstanding request in the opposite direction
/// whose reply type it is, which is exact for sequential exchanges between a pair of endpoints.
/// </remarks>
internal sealed class HarnessLinkPolicy : ILinkPolicy
{
    /// <summary>Most frame bytes the log retains for predicate filters and operation ids (older frames are released first).</summary>
    private const long MaxRetainedFrameBytes = 256L << 20;

    /// <summary>
    /// Frames at least this large (object payloads, snapshot chunks) are retained under <see cref="MaxRetainedBulkFrameBytes"/>
    /// instead: the log is part of the test process, and keeping hundreds of MiB of recent bulk data alive would make the
    /// process's live memory follow the data moved rather than the product's buffers (PERF-003).
    /// </summary>
    private const int BulkFrameBytes = 16 << 10;

    /// <summary>Most bytes of bulk frames the log retains (older bulk frames are released first).</summary>
    private const long MaxRetainedBulkFrameBytes = 8L << 20;

    /// <summary>Most outstanding requests remembered per direction for reply attribution.</summary>
    private const int MaxOutstandingPerLink = 4096;

    private readonly object _gate = new();
    private readonly ClusterRuntime _runtime;
    private readonly Dictionary<(string From, string To), LinkBlock> _blocks = [];
    private readonly HashSet<string> _isolated = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _partition = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _meshIndex = new(StringComparer.Ordinal);
    private readonly Dictionary<(string From, string To), TimeSpan> _latency = [];
    private readonly Dictionary<(string From, string To), long> _bandwidth = [];
    private readonly Dictionary<(string From, string To), long> _busyUntil = [];
    private readonly List<MessageRule> _rules = [];
    private readonly List<LogEntry> _log = [];
    private readonly Queue<LogEntry> _retained = new();
    private readonly Queue<LogEntry> _retainedBulk = new();
    private readonly Dictionary<(string Requester, string Responder), LinkedList<Outstanding>> _outstanding = [];
    private LinkMesh _mesh = LinkMesh.Full;
    private long _sequence;
    private long _retainedBytes;
    private long _retainedBulkBytes;
    private bool _applyLatency;

    public HarnessLinkPolicy(ClusterRuntime runtime)
    {
        _runtime = runtime;
    }

    /// <summary>
    /// When true (realtime transports) the policy implements <see cref="SetLatency"/> and <see cref="SetBandwidth"/> with
    /// delay decisions; the simulated network applies them itself.
    /// </summary>
    public bool ApplyLatency
    {
        get
        {
            lock (_gate)
                return _applyLatency;
        }
        set
        {
            lock (_gate)
                _applyLatency = value;
        }
    }

    // ------------------------------------------------------------------------------------------------ link state

    public void SetBlock(string from, string to, LinkBlock block)
    {
        lock (_gate)
            _blocks[(from, to)] = block;
    }

    public void RemoveBlock(string from, string to)
    {
        lock (_gate)
            _blocks.Remove((from, to));
    }

    public void Isolate(string name)
    {
        lock (_gate)
            _isolated.Add(name);
    }

    public void SetPartition(IReadOnlyList<IReadOnlyList<string>> groups)
    {
        // Validate completely before replacing anything: a rejected partition leaves the current one in force.
        var partition = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < groups.Count; i++)
        {
            foreach (string name in groups[i])
            {
                if (!partition.TryAdd(name, i))
                    throw new ArgumentException($"'{name}' is listed in more than one partition group.", nameof(groups));
            }
        }

        lock (_gate)
        {
            _partition.Clear();
            foreach (KeyValuePair<string, int> entry in partition)
                _partition[entry.Key] = entry.Value;
        }
    }

    /// <summary>Restricts links among <paramref name="members"/> (in index order) to <paramref name="mesh"/>.</summary>
    public void SetMesh(LinkMesh mesh, IReadOnlyList<string> members)
    {
        lock (_gate)
        {
            _mesh = mesh;
            _meshIndex.Clear();
            if (mesh == LinkMesh.Full)
                return;
            for (int i = 0; i < members.Count; i++)
                _meshIndex[members[i]] = i;
        }
    }

    /// <summary>Removes cuts, black holes, isolations, the partition and mesh restrictions.</summary>
    public void Heal()
    {
        lock (_gate)
        {
            _blocks.Clear();
            _isolated.Clear();
            _partition.Clear();
            _meshIndex.Clear();
            _mesh = LinkMesh.Full;
        }
    }

    public void SetLatency(string from, string to, TimeSpan latency)
    {
        lock (_gate)
        {
            if (latency <= TimeSpan.Zero)
                _latency.Remove((from, to));
            else
                _latency[(from, to)] = latency;
        }
    }

    public void SetBandwidth(string from, string to, long bytesPerSecond)
    {
        lock (_gate)
        {
            if (bytesPerSecond <= 0)
            {
                _bandwidth.Remove((from, to));
                _busyUntil.Remove((from, to));
            }
            else
            {
                _bandwidth[(from, to)] = bytesPerSecond;
            }
        }
    }

    /// <summary>True when messages can currently flow from <paramref name="from"/> to <paramref name="to"/>.</summary>
    public bool IsLinkUp(string from, string to)
    {
        lock (_gate)
            return BlockedLocked(from, to) is null;
    }

    public void AddRule(MessageRule rule)
    {
        lock (_gate)
            _rules.Add(rule);
    }

    /// <summary>True when an active rule of <paramref name="kind"/> exists (fast-path check of the interposition layer).</summary>
    public bool HasActiveRule(MessageRuleKind kind)
    {
        lock (_gate)
        {
            foreach (MessageRule rule in _rules)
            {
                if (rule.Kind == kind && rule.IsActive)
                    return true;
            }
        }

        return false;
    }

    /// <summary>The first active rule of <paramref name="kind"/> matching the message, without consuming it (or null).</summary>
    public MessageRule? FindActiveRule(MessageRuleKind kind, string from, string to, LinkDirection direction, ReadOnlySpan<byte> frame)
    {
        lock (_gate)
        {
            foreach (MessageRule rule in _rules)
            {
                if (rule.Kind == kind && rule.IsActive && rule.Filter.Matches(from, to, direction, frame))
                    return rule;
            }
        }

        return null;
    }

    /// <summary>
    /// Consumes one application of the first active rule of <paramref name="kind"/> matching the message (rules applied by
    /// the interposition layer rather than by transport decisions: reply drops, truncation, tampering).
    /// </summary>
    public MessageRule? TryTakeRule(MessageRuleKind kind, string from, string to, LinkDirection direction, ReadOnlySpan<byte> frame)
    {
        lock (_gate)
        {
            foreach (MessageRule rule in _rules)
            {
                if (rule.Kind != kind || !rule.IsActive)
                    continue;
                if (rule.Filter.Matches(from, to, direction, frame) && rule.TryConsume())
                    return rule;
            }
        }

        return null;
    }

    // ------------------------------------------------------------------------------------------------ ILinkPolicy

    /// <inheritdoc />
    /// <remarks>
    /// Directional like every other decision of the policy: only the direction the connection is opened in counts, so
    /// <c>Cut(a, b)</c>/<c>BlackHole(a, b)</c> stop <c>a</c> from reaching <c>b</c> but never stop <c>b</c> from opening a
    /// connection to <c>a</c> and delivering over it (HAR-006: only the <c>from -&gt; to</c> direction is affected; replies
    /// travelling back over such a connection are still dropped by <see cref="Decide"/>). Symmetric faults (isolation,
    /// partitions, mesh restrictions) block both directions anyway.
    /// </remarks>
    public bool CanConnect(string fromName, string toName)
    {
        lock (_gate)
            return BlockedLocked(fromName, toName) is null;
    }

    /// <inheritdoc />
    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix)
    {
        MessageType type = payloadPrefix.Length >= 2 ? (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(payloadPrefix) : 0;
        lock (_gate)
        {
            long sequence = ++_sequence;
            LinkDecision decision = LinkDecision.Deliver;
            if (BlockedLocked(fromName, toName) is { } block)
            {
                decision = block == LinkBlock.BlackHole ? LinkDecision.BlackHole : LinkDecision.Drop;
            }
            else
            {
                foreach (MessageRule rule in _rules)
                {
                    if (!rule.IsActive || rule.Kind is not (MessageRuleKind.Drop or MessageRuleKind.Duplicate or MessageRuleKind.Delay))
                        continue;
                    if (!rule.Filter.Matches(fromName, toName, direction, payloadPrefix) || !rule.TryConsume())
                        continue;
                    decision = rule.Kind switch
                    {
                        MessageRuleKind.Drop => LinkDecision.Drop,
                        MessageRuleKind.Duplicate => LinkDecision.Duplicate,
                        _ => LinkDecision.Delayed(rule.Delay),
                    };
                    break;
                }

                if (_applyLatency && decision.Kind == LinkDecisionKind.Deliver)
                {
                    TimeSpan extra = LinkDelayLocked(fromName, toName, payloadPrefix.Length);
                    if (extra > TimeSpan.Zero)
                        decision = LinkDecision.Delayed(extra);
                }
            }

            bool delivered = decision.Kind is LinkDecisionKind.Deliver or LinkDecisionKind.Delay or LinkDecisionKind.Duplicate;
            ulong correlation = (ulong)sequence;
            DfsOperationId? replyOperation = null;
            bool knownReplyOperation = false;
            if (direction == LinkDirection.Reply)
            {
                if (TakeOutstandingLocked(toName, fromName, type) is { } request)
                {
                    correlation = request.Correlation;
                    replyOperation = request.Operation;
                    knownReplyOperation = true;
                }
            }

            byte[] frame = payloadPrefix.ToArray();
            var entry = new LogEntry(sequence, fromName, toName, direction, type, correlation, frame.Length, decision.Kind, frame, replyOperation,
                knownReplyOperation);
            AppendLocked(entry);
            if (decision.Kind == LinkDecisionKind.Duplicate)
                AppendLocked(entry with { Sequence = ++_sequence });
            if (direction == LinkDirection.Request && delivered)
            {
                AddOutstandingLocked(fromName, toName, new Outstanding(correlation, type, entry));
                if (decision.Kind == LinkDecisionKind.Duplicate)
                    AddOutstandingLocked(fromName, toName, new Outstanding(correlation, type, entry));
            }

            return decision;
        }
    }

    // ------------------------------------------------------------------------------------------------ log

    /// <summary>Delivered messages (and their bytes) matching <paramref name="filter"/>.</summary>
    public long CountDelivered(MessageFilter filter, out long bytes)
    {
        long count = 0;
        bytes = 0;
        foreach (LogEntry entry in SnapshotLog())
        {
            if (!entry.IsDelivered || !Matches(filter, entry))
                continue;
            count++;
            bytes += entry.Bytes;
        }

        return count;
    }

    /// <summary>Every observation matching <paramref name="filter"/>, in order.</summary>
    public IReadOnlyList<MessageAttempt> GetAttempts(MessageFilter filter)
    {
        var result = new List<MessageAttempt>();
        foreach (LogEntry entry in SnapshotLog())
        {
            if (!Matches(filter, entry))
                continue;
            result.Add(new MessageAttempt(entry.Sequence, entry.From, entry.To, entry.Direction, entry.Type, entry.Correlation, OperationOf(entry),
                entry.Bytes, entry.Decision));
        }

        return result;
    }

    public void ResetLog()
    {
        lock (_gate)
        {
            _log.Clear();
            _retained.Clear();
            _retainedBytes = 0;
            _retainedBulk.Clear();
            _retainedBulkBytes = 0;
        }
    }

    private static bool Matches(MessageFilter filter, LogEntry entry)
    {
        if (filter.Predicate is null)
            return filter.Matches(entry.From, entry.To, entry.Direction, entry.Type, null);
        byte[]? frame = entry.Frame;
        return frame is not null && filter.Matches(entry.From, entry.To, entry.Direction, frame);
    }

    private static DfsOperationId? OperationOf(LogEntry entry)
    {
        if (entry.Direction == LinkDirection.Reply)
            return entry.KnownReplyOperation ? entry.ReplyOperation : null;
        return OperationOfFrame(entry.Frame);
    }

    /// <summary>The operation id a request frame carries (QueryOperation: its target), or null.</summary>
    internal static DfsOperationId? OperationOfFrame(byte[]? frame)
    {
        if (frame is null || frame.Length < 2)
            return null;
        var type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(frame);
        if (!MessageCodec.IsKnown(type) || MessageTypes.IsReply(type))
            return null;
        try
        {
            IWireMessage message = MessageCodec.Decode(frame);
            if (message is QueryOperationMessage query)
                return query.Target;
            if (message is IVolumeRequest request && !request.Header.Operation.IsEmpty)
                return request.Header.Operation;
            return null;
        }
        catch (Exception ex) when (ex is DfsException or ArgumentException or InvalidOperationException or FormatException)
        {
            return null;
        }
    }

    private LogEntry[] SnapshotLog()
    {
        lock (_gate)
            return [.. _log];
    }

    private void AppendLocked(LogEntry entry)
    {
        _log.Add(entry);
        if (entry.Frame is null)
            return;
        if (entry.Frame.Length >= BulkFrameBytes)
            Retain(_retainedBulk, ref _retainedBulkBytes, MaxRetainedBulkFrameBytes, entry);
        else
            Retain(_retained, ref _retainedBytes, MaxRetainedFrameBytes, entry);

        static void Retain(Queue<LogEntry> queue, ref long retainedBytes, long budget, LogEntry entry)
        {
            queue.Enqueue(entry);
            retainedBytes += entry.Frame!.Length;
            while (retainedBytes > budget && queue.Count > 0)
            {
                LogEntry oldest = queue.Dequeue();
                if (oldest.Frame is { } released)
                {
                    retainedBytes -= released.Length;
                    oldest.ReleaseFrame();
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private LinkBlock? BlockedLocked(string from, string to)
    {
        if (_isolated.Contains(from) || _isolated.Contains(to))
            return LinkBlock.Cut;
        if (_blocks.TryGetValue((from, to), out LinkBlock block))
            return block;
        if (_partition.TryGetValue(from, out int a) && _partition.TryGetValue(to, out int b) && a != b)
            return LinkBlock.Cut;
        if (_mesh != LinkMesh.Full && _meshIndex.TryGetValue(from, out int i) && _meshIndex.TryGetValue(to, out int j) && !MeshAllows(i, j))
            return LinkBlock.Cut;
        return null;
    }

    private bool MeshAllows(int i, int j)
    {
        if (i == j)
            return true;
        int n = _meshIndex.Count;
        return _mesh switch
        {
            LinkMesh.Star => i == 0 || j == 0,
            LinkMesh.Line => Math.Abs(i - j) == 1,
            LinkMesh.Ring => Math.Abs(i - j) == 1 || (n > 2 && Math.Abs(i - j) == n - 1),
            _ => true,
        };
    }

    private TimeSpan LinkDelayLocked(string from, string to, int bytes)
    {
        TimeSpan delay = _latency.TryGetValue((from, to), out TimeSpan latency) ? latency : TimeSpan.Zero;
        if (_bandwidth.TryGetValue((from, to), out long bps) && bps > 0)
        {
            long now = _runtime.Time.GetTimestamp();
            long transferTicks = (long)Math.Ceiling(bytes * (double)TimeSpan.TicksPerSecond / bps);
            long nowTicks = (long)(now * ((double)TimeSpan.TicksPerSecond / _runtime.Time.TimestampFrequency));
            long start = _busyUntil.TryGetValue((from, to), out long busy) && busy > nowTicks ? busy : nowTicks;
            long end = start + transferTicks;
            _busyUntil[(from, to)] = end;
            delay += TimeSpan.FromTicks(end - nowTicks);
        }

        return delay;
    }

    private void AddOutstandingLocked(string requester, string responder, Outstanding request)
    {
        if (!_outstanding.TryGetValue((requester, responder), out LinkedList<Outstanding>? list))
            _outstanding[(requester, responder)] = list = new LinkedList<Outstanding>();
        list.AddLast(request);
        while (list.Count > MaxOutstandingPerLink)
            list.RemoveFirst();
    }

    private Outstanding? TakeOutstandingLocked(string requester, string responder, MessageType replyType)
    {
        if (!_outstanding.TryGetValue((requester, responder), out LinkedList<Outstanding>? list))
            return null;
        for (LinkedListNode<Outstanding>? node = list.First; node is not null; node = node.Next)
        {
            MessageType requestType = node.Value.Type;
            bool fits = replyType == MessageType.ErrorReply
                || (MessageCodec.IsKnown(requestType) && !MessageTypes.IsReply(requestType) && SafeReplyOf(requestType) == replyType);
            if (!fits)
                continue;
            list.Remove(node);
            return node.Value with { Operation = OperationOfFrame(node.Value.Request.Frame) };
        }

        return null;
    }

    private static MessageType? SafeReplyOf(MessageType request)
    {
        try
        {
            return MessageTypes.ReplyOf(request);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private sealed record Outstanding(ulong Correlation, MessageType Type, LogEntry Request)
    {
        public DfsOperationId? Operation { get; init; }
    }

    private sealed record LogEntry(long Sequence, string From, string To, LinkDirection Direction, MessageType Type, ulong Correlation, int Bytes,
        LinkDecisionKind Decision, byte[]? InitialFrame, DfsOperationId? ReplyOperation, bool KnownReplyOperation)
    {
        private byte[]? _frame = InitialFrame;

        /// <summary>
        /// The observed frame, stored only in the releasable field: a compiler-generated positional property would keep a
        /// second reference, and <see cref="ReleaseFrame"/> would then never free the bytes (the retention bound was moot).
        /// </summary>
        public byte[]? InitialFrame
        {
            get => _frame;
            init => _frame = value;
        }

        public byte[]? Frame => Volatile.Read(ref _frame);

        public bool IsDelivered => Decision is LinkDecisionKind.Deliver or LinkDecisionKind.Delay or LinkDecisionKind.Duplicate;

        public void ReleaseFrame() => Volatile.Write(ref _frame, null);
    }
}
