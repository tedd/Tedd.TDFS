using System.Buffers.Binary;
using System.Collections.Concurrent;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Transport;

/// <summary>An <see cref="IRpcHandler"/> driven by a delegate; records every invocation.</summary>
internal sealed class DelegateHandler : IRpcHandler
{
    private readonly Func<IRpcConnection, ReadOnlyMemory<byte>, bool, CancellationToken, ValueTask<ReadOnlyMemory<byte>?>> _handle;

    public DelegateHandler(Func<IRpcConnection, ReadOnlyMemory<byte>, bool, CancellationToken, ValueTask<ReadOnlyMemory<byte>?>> handle)
    {
        _handle = handle;
    }

    public ConcurrentQueue<(byte[] Payload, bool ExpectsReply, int ThreadId)> Received { get; } = new();

    public int Count => Received.Count;

    /// <summary>Echoes requests; records one-way messages.</summary>
    public static DelegateHandler Echo() => new((_, request, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray()));

    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        Received.Enqueue((request.ToArray(), expectsReply, Environment.CurrentManagedThreadId));
        return _handle(connection, request, expectsReply, ct);
    }
}

/// <summary>A link policy driven by delegates; records every decision request.</summary>
internal sealed class ScriptedLinkPolicy : ILinkPolicy
{
    private readonly object _gate = new();
    private Func<string, string, LinkDirection, byte[], LinkDecision> _decide = (_, _, _, _) => LinkDecision.Deliver;
    private Func<string, string, bool> _canConnect = (_, _) => true;

    public ConcurrentQueue<(string From, string To, LinkDirection Direction, byte[] Payload)> Decisions { get; } = new();

    public ConcurrentQueue<(string From, string To)> ConnectChecks { get; } = new();

    public void OnDecide(Func<string, string, LinkDirection, byte[], LinkDecision> decide)
    {
        lock (_gate)
        {
            _decide = decide;
        }
    }

    public void OnCanConnect(Func<string, string, bool> canConnect)
    {
        lock (_gate)
        {
            _canConnect = canConnect;
        }
    }

    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix)
    {
        byte[] payload = payloadPrefix.ToArray();
        Decisions.Enqueue((fromName, toName, direction, payload));
        Func<string, string, LinkDirection, byte[], LinkDecision> decide;
        lock (_gate)
        {
            decide = _decide;
        }

        return decide(fromName, toName, direction, payload);
    }

    public bool CanConnect(string fromName, string toName)
    {
        ConnectChecks.Enqueue((fromName, toName));
        Func<string, string, bool> canConnect;
        lock (_gate)
        {
            canConnect = _canConnect;
        }

        return canConnect(fromName, toName);
    }
}

/// <summary>A manually advanced <see cref="TimeProvider"/> for deterministic timeout tests.</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private static readonly DateTimeOffset Epoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private readonly object _gate = new();
    private readonly List<ManualTimer> _timers = [];
    private long _now;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <summary>Timers that are scheduled to fire.</summary>
    public int ScheduledTimers
    {
        get
        {
            lock (_gate)
            {
                return _timers.Count(t => t.Due != long.MaxValue);
            }
        }
    }

    public override DateTimeOffset GetUtcNow() => Epoch + TimeSpan.FromTicks(Interlocked.Read(ref _now));

    public override long GetTimestamp() => Interlocked.Read(ref _now);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(this, callback, state);
        lock (_gate)
        {
            _timers.Add(timer);
        }

        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Moves time forward and fires every timer that became due (callbacks run on the caller's thread).</summary>
    public void Advance(TimeSpan by)
    {
        long target = Interlocked.Read(ref _now) + by.Ticks;
        while (true)
        {
            ManualTimer? next = null;
            lock (_gate)
            {
                foreach (ManualTimer timer in _timers)
                {
                    if (timer.Due <= target && (next is null || timer.Due < next.Due))
                        next = timer;
                }

                if (next is null)
                {
                    Interlocked.Exchange(ref _now, target);
                    return;
                }

                Interlocked.Exchange(ref _now, Math.Max(Interlocked.Read(ref _now), next.Due));
                next.Due = next.Period > 0 ? next.Due + next.Period : long.MaxValue;
            }

            next.Fire();
        }
    }

    private void Remove(ManualTimer timer)
    {
        lock (_gate)
        {
            _timers.Remove(timer);
        }
    }

    private sealed class ManualTimer : ITimer
    {
        private readonly ManualTimeProvider _owner;
        private readonly TimerCallback _callback;
        private readonly object? _state;

        public ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state)
        {
            _owner = owner;
            _callback = callback;
            _state = state;
        }

        public long Due { get; set; } = long.MaxValue;

        public long Period { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (_owner._gate)
            {
                Due = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : _owner.GetTimestamp() + dueTime.Ticks;
                Period = period == Timeout.InfiniteTimeSpan ? 0 : period.Ticks;
            }

            return true;
        }

        public void Fire() => _callback(_state);

        public void Dispose() => _owner.Remove(this);

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>Helpers shared by the transport tests.</summary>
internal static class TransportTestData
{
    /// <summary>Generous bound for anything that must happen "soon" in a realtime test.</summary>
    public static readonly TimeSpan Soon = TimeSpan.FromSeconds(10);

    /// <summary>A frame-like payload: <c>[u16 type][u32 sequence][filler]</c>.</summary>
    public static byte[] Frame(ushort type, uint sequence, int fillerBytes = 0)
    {
        byte[] payload = new byte[6 + fillerBytes];
        BinaryPrimitives.WriteUInt16LittleEndian(payload, type);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(2), sequence);
        for (int i = 0; i < fillerBytes; i++)
            payload[6 + i] = (byte)(sequence + i);
        return payload;
    }

    public static ushort TypeOf(ReadOnlySpan<byte> frame) => BinaryPrimitives.ReadUInt16LittleEndian(frame);

    public static uint SequenceOf(ReadOnlySpan<byte> frame) => BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(2));

    /// <summary>Deterministic pseudo-random bytes.</summary>
    public static byte[] Pattern(int length, int seed)
    {
        byte[] data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    /// <summary>Waits until <paramref name="condition"/> holds or <see cref="Soon"/> passes.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow + Soon;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out waiting for: {what}");
            await Task.Delay(5);
        }
    }
}
