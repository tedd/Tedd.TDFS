using System.Globalization;
using System.Text;

namespace Tedd.Tdfs.Testing.SnpApp;

/// <summary>One event of an <see cref="AdapterTimeline"/>.</summary>
/// <param name="Sequence">Strictly increasing position in the timeline (1-based); defines the observed order.</param>
/// <param name="Source">Who recorded the event (adapter id, "server", "app", "test", ...).</param>
/// <param name="Event">Event name ("prepare.start", "prepare.done", "capture.applied", ...).</param>
/// <param name="At">Wall time of the timeline's clock when recorded.</param>
/// <param name="Timestamp">Monotonic timestamp of the timeline's clock when recorded (<see cref="TimeProvider.GetTimestamp"/>).</param>
/// <param name="Detail">Optional detail (node name, exception type, ...).</param>
public sealed record TimelineEvent(long Sequence, string Source, string Event, DateTimeOffset At, long Timestamp, string? Detail);

/// <summary>
/// Thread-safe, totally ordered log of callback and probe events used to assert the relative order of adapter callbacks,
/// the snapshot capture on the server (recorded by fault-site probes) and backup transfer (APP-002, APP-003, APP-005,
/// APP-007). Every <see cref="Record"/> call takes the next sequence number under a lock, so if one event is recorded
/// before another starts being recorded (in real or simulated time), its sequence is smaller.
/// </summary>
/// <remarks>Uses only the supplied <see cref="TimeProvider"/>; safe inside the deterministic simulation.</remarks>
public sealed class AdapterTimeline
{
    private readonly object _lock = new();
    private readonly List<TimelineEvent> _events = [];
    private long _sequence;

    /// <summary>Creates a timeline using <paramref name="time"/> (default <see cref="TimeProvider.System"/>).</summary>
    public AdapterTimeline(TimeProvider? time = null)
    {
        Time = time ?? TimeProvider.System;
    }

    /// <summary>The clock used for <see cref="TimelineEvent.At"/> and <see cref="TimelineEvent.Timestamp"/>.</summary>
    public TimeProvider Time { get; }

    /// <summary>All events in sequence order.</summary>
    public IReadOnlyList<TimelineEvent> Events
    {
        get
        {
            lock (_lock)
                return _events.ToArray();
        }
    }

    /// <summary>Records an event and returns its sequence number.</summary>
    public long Record(string source, string eventName, string? detail = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentException.ThrowIfNullOrEmpty(eventName);
        DateTimeOffset at = Time.GetUtcNow();
        long timestamp = Time.GetTimestamp();
        lock (_lock)
        {
            long sequence = ++_sequence;
            _events.Add(new TimelineEvent(sequence, source, eventName, at, timestamp, detail));
            return sequence;
        }
    }

    /// <summary>The first event with this source and name, or null.</summary>
    public TimelineEvent? Find(string source, string eventName)
    {
        lock (_lock)
            return _events.FirstOrDefault(e => Matches(e, source, eventName));
    }

    /// <summary>The last event with this source and name, or null.</summary>
    public TimelineEvent? FindLast(string source, string eventName)
    {
        lock (_lock)
            return _events.LastOrDefault(e => Matches(e, source, eventName));
    }

    /// <summary>The first event with this source and name; fails the test (with the full timeline) when absent.</summary>
    public TimelineEvent Get(string source, string eventName)
    {
        TimelineEvent? e = Find(source, eventName);
        if (e is null)
            Assert.Fail($"Timeline has no event {source}:{eventName}. Timeline:{Environment.NewLine}{this}");
        return e!;
    }

    /// <summary>True when an event with this source and name was recorded.</summary>
    public bool Has(string source, string eventName) => Find(source, eventName) is not null;

    /// <summary>All events with this name (any source), in sequence order.</summary>
    public IReadOnlyList<TimelineEvent> WithName(string eventName)
    {
        lock (_lock)
            return _events.Where(e => string.Equals(e.Event, eventName, StringComparison.Ordinal)).ToArray();
    }

    /// <summary>Fails the test unless <paramref name="earlier"/> was recorded strictly before <paramref name="later"/>.</summary>
    public void AssertBefore(TimelineEvent earlier, TimelineEvent later, string because)
    {
        ArgumentNullException.ThrowIfNull(earlier);
        ArgumentNullException.ThrowIfNull(later);
        if (earlier.Sequence >= later.Sequence)
        {
            Assert.Fail(
                $"Expected {earlier.Source}:{earlier.Event} (#{earlier.Sequence}) before {later.Source}:{later.Event} (#{later.Sequence}): {because}." +
                $"{Environment.NewLine}Timeline:{Environment.NewLine}{this}");
        }
    }

    /// <summary>Elapsed time between two events on the timeline clock.</summary>
    public TimeSpan Elapsed(TimelineEvent from, TimelineEvent to) => Time.GetElapsedTime(from.Timestamp, to.Timestamp);

    /// <inheritdoc />
    public override string ToString()
    {
        var sb = new StringBuilder();
        foreach (TimelineEvent e in Events)
        {
            sb.Append(CultureInfo.InvariantCulture, $"  #{e.Sequence} {e.At:HH:mm:ss.fff} {e.Source}:{e.Event}");
            if (e.Detail is not null)
                sb.Append(" (").Append(e.Detail).Append(')');
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static bool Matches(TimelineEvent e, string source, string eventName) =>
        string.Equals(e.Source, source, StringComparison.Ordinal) && string.Equals(e.Event, eventName, StringComparison.Ordinal);
}
