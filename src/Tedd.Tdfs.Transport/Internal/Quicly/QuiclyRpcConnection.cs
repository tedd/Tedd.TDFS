using System.Collections.Concurrent;
using Tedd.Quicly.Client;
using Tedd.Quicly.Core.Session;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>What a pump send pass achieved for one connection.</summary>
internal enum SendOutcome
{
    /// <summary>Every queued frame was handed to Quicly (or the connection closed and its queue was failed).</summary>
    Drained,

    /// <summary>Quicly refused a frame for now (queue or buffer budget full); retry after completions.</summary>
    Blocked,

    /// <summary>The pass's frame budget ran out with frames still queued; continue in the next pass at once.</summary>
    More,
}

/// <summary>
/// One QUIC RPC connection: an outbound Quicly client connection or an inbound server peer. Senders on any thread
/// enqueue envelope messages; the transport's pump thread fragments and hands them to Quicly, and Quicly delivers
/// received frames to <see cref="OnMessage"/> on the peer's game thread (the pump, or the connecting thread while a
/// client connects).
/// </summary>
internal sealed class QuiclyRpcConnection : RpcConnectionBase
{
    private const int MaxEarlyMessages = 1024;

    private readonly QuiclyRpcTransport _owner;
    private readonly ConcurrentQueue<OutboundMessage> _control = new();
    private readonly ConcurrentQueue<OutboundMessage> _data = new();
    private readonly QuiclyReassembler _reassembler;
    private int _sendScheduled;

    // Game-thread (pump) state.
    private OutboundMessage? _currentData;
    private List<(RpcMessageKind Kind, ulong Correlation, byte[] Payload)>? _early;

    public QuiclyRpcConnection(QuiclyRpcTransport owner, long id, RpcAddress remote, string? remoteName, bool isClientSide)
        : base(id, remote, remoteName)
    {
        _owner = owner;
        IsClientSide = isClientSide;
        _reassembler = new QuiclyReassembler(owner.Options.MaxMessageBytes, owner.Options.MaxReassemblyBytesPerConnection);
        if (isClientSide)
            Preamble = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>True for a connection this transport opened (a Quicly client).</summary>
    public bool IsClientSide { get; }

    /// <summary>Completes with the server's network identity when its preamble arrives (client side only).</summary>
    public TaskCompletionSource<string>? Preamble { get; }

    /// <summary>The Quicly peer (pump thread; null until attached and after release).</summary>
    public QuiclyPeer? Peer { get; set; }

    /// <summary>The Quicly client of an outbound connection (pump thread; null until attached and after disposal).</summary>
    public QuiclyClient? Client { get; set; }

    /// <summary>The client's reference on the shared client buffer pool.</summary>
    public ClientLease? Lease { get; set; }

    /// <summary>
    /// Pump-clock deadline of the current close step: the delivery wait of a graceful close (<see cref="PendingClose"/>),
    /// then the Quicly close itself.
    /// </summary>
    public long CloseDeadline { get; set; }

    /// <summary>
    /// The Quicly close reason of a graceful local close that waits until every message already handed to Quicly was
    /// delivered (pump thread); null when no close is pending.
    /// </summary>
    public CloseReason? PendingClose { get; set; }

    /// <summary>True when the pump polled or sent to this client in the current pass, so it must flush it (pump thread).</summary>
    public bool NeedsFlush { get; set; }

    /// <summary>The fragment reassembler (game thread).</summary>
    public QuiclyReassembler Reassembler => _reassembler;

    /// <summary>True while frames wait to be handed to Quicly.</summary>
    public bool HasOutbound => !_control.IsEmpty || !_data.IsEmpty || _currentData is not null;

    protected override string LocalName => _owner.LocalName;

    protected override TimeProvider Time => _owner.Options.TimeProvider;

    protected override ILinkPolicy Policy => _owner.Policy;

    protected override IRpcHandler? Handler => _owner.Handler;

    /// <summary>
    /// Receives one Quicly message on channel 2 or 3 (game thread). The payload is only valid during the call: it is
    /// parsed, copied and either reassembled or posted for delivery.
    /// </summary>
    public void OnMessage(QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload)
    {
        if (!IsOpen)
            return;
        try
        {
            if (!QuiclyEnvelope.TryParse(payload, out EnvelopeHeader envelope, out ReadOnlySpan<byte> chunk, out string? error))
                throw new QuiclyProtocolException(error!);
            if (envelope.Kind == RpcMessageKind.Preamble)
            {
                OnPreamble(chunk);
                return;
            }

            byte[]? message = envelope.FragmentCount == 1 ? chunk.ToArray() : _reassembler.Add(in envelope, chunk);
            if (message is null)
                return;
            if (message.Length > _owner.Options.MaxMessageBytes)
                throw new QuiclyProtocolException($"A message exceeds {_owner.Options.MaxMessageBytes} bytes.");

            if (RemoteName is null)
            {
                // A server message that overtook the server's preamble (possible for fragmented messages, which use the
                // unordered channel): hold it until the sender's identity is known.
                _early ??= [];
                if (_early.Count >= MaxEarlyMessages)
                    throw new QuiclyProtocolException("Too many messages arrived before the preamble.");
                _early.Add((envelope.Kind, envelope.Correlation, message));
                return;
            }

            PostInbound(envelope.Kind, envelope.Correlation, message);
        }
        catch (QuiclyProtocolException ex)
        {
            _owner.Report($"Protocol violation on the connection to {Remote}: {ex.Message}", null);
            TryClose(new RpcConnectionException($"The connection to {Remote} was closed after a protocol violation: {ex.Message}", ex), RpcCloseOrigin.Fault);
        }
    }

    /// <summary>Queues this end's preamble (server side, sent first on the ordered channel).</summary>
    public void EnqueuePreamble(string localName)
    {
        _control.Enqueue(new OutboundMessage(RpcMessageKind.Preamble, 0, QuiclyEnvelope.EncodeName(localName), null));
        _owner.ScheduleSend(this);
    }

    /// <summary>
    /// Hands queued frames to Quicly (pump thread): first the control lane (single-frame messages), then the data lane
    /// (fragments, one message after the other).
    /// </summary>
    /// <param name="scratch">Pump-owned frame buffer of <see cref="QuiclyEnvelope.MaxFrameSize"/> bytes.</param>
    /// <param name="frameBudget">Most frames to admit in this pass.</param>
    public SendOutcome PumpSends(byte[] scratch, int frameBudget)
    {
        if (!IsOpen)
        {
            FailOutbound();
            return SendOutcome.Drained;
        }

        QuiclyPeer? peer = Peer;
        if (peer is null)
            return HasOutbound ? SendOutcome.Blocked : SendOutcome.Drained;

        int frames = 0;
        NeedsFlush = true;
        while (_control.TryPeek(out OutboundMessage? message))
        {
            if (frames >= frameBudget)
                return SendOutcome.More;
            SendStatus status = SendFrame(peer, QuiclyEnvelope.ControlChannel, message, 0, scratch);
            if (status != SendStatus.Admitted)
                return OnRefused(status);
            _control.TryDequeue(out _);
            message.Complete();
            frames++;
        }

        while (true)
        {
            OutboundMessage? message = _currentData;
            if (message is null)
            {
                if (!_data.TryDequeue(out message))
                    return SendOutcome.Drained;
                _currentData = message;
            }

            while (message.NextFragment < message.FragmentCount)
            {
                if (frames >= frameBudget)
                    return SendOutcome.More;
                SendStatus status = SendFrame(peer, QuiclyEnvelope.DataChannel, message, message.NextFragment, scratch);
                if (status != SendStatus.Admitted)
                    return OnRefused(status);
                message.NextFragment++;
                frames++;
            }

            _currentData = null;
            message.Complete();
        }
    }

    /// <summary>
    /// Marks the connection as no longer scheduled for sending (pump thread). Returns true when frames arrived meanwhile
    /// and the pump must keep the connection in its send list.
    /// </summary>
    public bool ReleaseSendSchedule()
    {
        Volatile.Write(ref _sendScheduled, 0);
        return HasOutbound && Interlocked.Exchange(ref _sendScheduled, 1) == 0;
    }

    /// <summary>Claims the send schedule for a producer; true when the caller must hand the connection to the pump.</summary>
    public bool TryClaimSendSchedule() => Interlocked.Exchange(ref _sendScheduled, 1) == 0;

    /// <summary>Fails every queued message (the connection is closed). Safe on any thread for the queues; the
    /// in-progress data message is only touched on the pump thread or after the pump exited.</summary>
    public void FailOutbound()
    {
        RpcConnectionException? failure = null;
        if (_currentData is { } current)
        {
            _currentData = null;
            current.Fail(failure ??= CreateClosedException());
        }

        while (_control.TryDequeue(out OutboundMessage? message))
            message.Fail(failure ??= CreateClosedException());
        while (_data.TryDequeue(out OutboundMessage? message))
            message.Fail(failure ??= CreateClosedException());
    }

    protected override void ValidatePayload(int length)
    {
        if (length > _owner.Options.MaxMessageBytes)
            throw new ArgumentException($"The message is {length} bytes; the transport accepts at most {_owner.Options.MaxMessageBytes}.", "payload");
    }

    protected override ValueTask TransmitAsync(RpcMessageKind kind, ulong correlation, ReadOnlyMemory<byte> payload, bool awaitTransmission)
    {
        if (!IsOpen)
            throw CreateClosedException();
        TaskCompletionSource? completion = awaitTransmission ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : null;
        var message = new OutboundMessage(kind, correlation, payload.ToArray(), completion);
        (message.FragmentCount == 1 ? _control : _data).Enqueue(message);
        if (!IsOpen)
        {
            // Closed while enqueuing: the close may already have drained the queues, so fail what is left.
            DrainQueuesAfterClose();
        }
        else
        {
            _owner.ScheduleSend(this);
        }

        return completion is null ? ValueTask.CompletedTask : new ValueTask(completion.Task);
    }

    protected override void OnClosed(RpcCloseOrigin origin, Exception? error)
    {
        Preamble?.TrySetException(CreateClosedException());
        DrainQueuesAfterClose();
        _owner.OnConnectionClosed(this, origin, error);
    }

    protected override void ReportFault(string message, Exception? exception) => _owner.Report($"{message} (connection to {Remote})", exception);

    private void DrainQueuesAfterClose()
    {
        RpcConnectionException? failure = null;
        while (_control.TryDequeue(out OutboundMessage? message))
            message.Fail(failure ??= CreateClosedException());
        while (_data.TryDequeue(out OutboundMessage? message))
            message.Fail(failure ??= CreateClosedException());
    }

    private void OnPreamble(ReadOnlySpan<byte> chunk)
    {
        if (!IsClientSide || RemoteName is not null)
            throw new QuiclyProtocolException("Unexpected preamble.");
        if (!QuiclyEnvelope.TryDecodeName(chunk, out string? name))
            throw new QuiclyProtocolException("The preamble does not carry a valid network identity.");
        RemoteName = name;
        if (_early is { } early)
        {
            _early = null;
            foreach ((RpcMessageKind kind, ulong correlation, byte[] payload) in early)
                PostInbound(kind, correlation, payload);
        }

        Preamble!.TrySetResult(name!);
    }

    private SendOutcome OnRefused(SendStatus status)
    {
        if (status is SendStatus.QueueFull or SendStatus.OutOfBuffers)
            return SendOutcome.Blocked;
        _owner.Report($"Quicly refused a frame for {Remote} ({status}); the connection is closed.", null);
        TryClose(new RpcConnectionException($"Sending to {Remote} failed ({status})."), RpcCloseOrigin.Fault);
        FailOutbound();
        return SendOutcome.Drained;
    }

    private static SendStatus SendFrame(QuiclyPeer peer, ushort channel, OutboundMessage message, int fragment, byte[] scratch)
    {
        int offset = fragment * QuiclyEnvelope.MaxChunkSize;
        int length = Math.Min(QuiclyEnvelope.MaxChunkSize, message.Payload.Length - offset);
        int frameLength = QuiclyEnvelope.WriteFrame(scratch, message.Kind, message.Correlation, (uint)fragment, (uint)message.FragmentCount,
            message.Payload.AsSpan(offset, length));
        return peer.SendCopy(new SendHeader(channel), scratch.AsSpan(0, frameLength)).Status;
    }

    /// <summary>A message waiting to be handed to Quicly.</summary>
    private sealed class OutboundMessage
    {
        public OutboundMessage(RpcMessageKind kind, ulong correlation, byte[] payload, TaskCompletionSource? completion)
        {
            Kind = kind;
            Correlation = correlation;
            Payload = payload;
            Completion = completion;
            FragmentCount = QuiclyEnvelope.FragmentCount(payload.Length);
        }

        public RpcMessageKind Kind { get; }

        public ulong Correlation { get; }

        public byte[] Payload { get; }

        public int FragmentCount { get; }

        public int NextFragment { get; set; }

        private TaskCompletionSource? Completion { get; }

        public void Complete() => Completion?.TrySetResult();

        public void Fail(Exception error) => Completion?.TrySetException(error);
    }
}
