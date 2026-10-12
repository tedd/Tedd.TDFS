using System.Globalization;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>Captures matching messages until they are released or dropped.</summary>
/// <remarks>
/// A held message has been delivered by the network to its receiver, which does not process it until it is released:
/// requests and one-way messages are held before the receiving node's handler runs, replies after the responder produced
/// them and before they are sent back. Every transmission matching <see cref="Filter"/> is held (SDK retries included)
/// until <see cref="ReleaseAll"/>, <see cref="DropAll"/> or <see cref="Dispose"/>, which also end the hold.
/// <see cref="Captured"/> keeps every frame captured during the hold's lifetime (for <see cref="TestNetwork.ReplayAsync"/>).
/// </remarks>
public sealed class MessageHold : IDisposable
{
    private readonly object _gate = new();
    private readonly List<HeldMessage> _held = [];
    private readonly List<CapturedMessage> _captured = [];
    private readonly NetworkFabric? _fabric;
    private TaskCompletionSource? _changed;
    private bool _active = true;

    internal MessageHold(MessageFilter filter)
        : this(filter, null)
    {
    }

    internal MessageHold(MessageFilter filter, NetworkFabric? fabric)
    {
        ArgumentNullException.ThrowIfNull(filter);
        Filter = filter;
        _fabric = fabric;
    }

    /// <summary>The filter selecting held messages.</summary>
    public MessageFilter Filter { get; }

    /// <summary>Messages currently held.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
                return _held.Count;
        }
    }

    /// <summary>All frames captured by this hold (for replay tests).</summary>
    public IReadOnlyList<CapturedMessage> Captured
    {
        get
        {
            lock (_gate)
                return [.. _captured];
        }
    }

    /// <summary>True while the hold captures new messages.</summary>
    internal bool IsActive
    {
        get
        {
            lock (_gate)
                return _active;
        }
    }

    private ClusterRuntime Runtime => _fabric?.Runtime ?? throw new InvalidOperationException("The hold is not attached to a network.");

    /// <summary>Waits until at least <paramref name="count"/> messages are held.</summary>
    /// <exception cref="TimeoutException">Fewer messages were held after <paramref name="timeout"/> of cluster time.</exception>
    public async Task WaitForAsync(int count, TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ClusterRuntime runtime = Runtime;
        long start = runtime.Time.GetTimestamp();
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (_held.Count >= count)
                    return;
                _changed ??= runtime.CreateCompletion();
                changed = _changed.Task;
            }

            TimeSpan remaining = timeout == Timeout.InfiniteTimeSpan ? Timeout.InfiniteTimeSpan : timeout - runtime.Time.GetElapsedTime(start);
            if ((remaining != Timeout.InfiniteTimeSpan && remaining <= TimeSpan.Zero) || !await runtime.WaitAsync(changed, remaining))
            {
                throw new TimeoutException(string.Create(CultureInfo.InvariantCulture,
                    $"The hold has {Count} message(s) after {timeout}; expected at least {count} (captured {Captured.Count} in total)."));
            }
        }
    }

    /// <summary>Delivers all held messages (optionally in reverse order) and stops holding.</summary>
    public void ReleaseAll(bool reverseOrder = false) => Finish(deliver: true, reverseOrder);

    /// <summary>Discards all held messages and stops holding.</summary>
    public void DropAll() => Finish(deliver: false, reverseOrder: false);

    /// <summary>Releases remaining messages and stops holding.</summary>
    public void Dispose() => Finish(deliver: true, reverseOrder: false);

    /// <summary>
    /// Captures a message when the hold is active and its filter matches; the returned entry is decided by
    /// <see cref="ReleaseAll"/> (true) or <see cref="DropAll"/> (false).
    /// </summary>
    internal HeldMessage? TryCapture(string from, string to, LinkDirection direction, ReadOnlySpan<byte> frame, Func<TaskCompletionSource<bool>> createDecision,
        object? origin)
    {
        if (!Filter.Matches(from, to, direction, frame))
            return null;
        TaskCompletionSource? changed;
        HeldMessage held;
        lock (_gate)
        {
            if (!_active)
                return null;
            MessageType type = frame.Length >= 2 ? (MessageType)System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(frame) : 0;
            var captured = new CapturedMessage(from, to, direction, type, frame.ToArray());
            held = new HeldMessage(captured, createDecision(), origin);
            _held.Add(held);
            _captured.Add(captured);
            changed = _changed;
            _changed = null;
        }

        if (changed is not null)
            Runtime.Complete(() => changed.TrySetResult());
        return held;
    }

    private void Finish(bool deliver, bool reverseOrder)
    {
        List<HeldMessage> held;
        TaskCompletionSource? changed;
        lock (_gate)
        {
            if (!_active && _held.Count == 0)
                return;
            _active = false;
            held = [.. _held];
            _held.Clear();
            changed = _changed;
            _changed = null;
        }

        if (reverseOrder)
            held.Reverse();
        _fabric?.Trace($"hold {(deliver ? "released" : "dropped")} {held.Count.ToString(CultureInfo.InvariantCulture)} message(s){(reverseOrder ? " in reverse order" : string.Empty)}");
        foreach (HeldMessage message in held)
            Runtime.Complete(() => message.Decision.TrySetResult(deliver));
        if (changed is not null)
            Runtime.Complete(() => changed.TrySetResult());
    }
}

/// <summary>One message held by a <see cref="MessageHold"/>; its decision completes with true (deliver) or false (drop).</summary>
/// <param name="Message">The captured frame.</param>
/// <param name="Decision">Completed when the hold releases or drops the message.</param>
/// <param name="Origin">The harness connection the message travels on (replay support), when known.</param>
internal sealed record HeldMessage(CapturedMessage Message, TaskCompletionSource<bool> Decision, object? Origin);
