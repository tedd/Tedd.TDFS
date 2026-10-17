namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Round-trip estimate of one endpoint (Jacobson/Karels: smoothed RTT and mean deviation), fed by every reply received
/// from it — including replies that arrive after their attempt gave up waiting, and the handshake's — and shared by the
/// endpoint's successive connections, so a link that became slow is known to be slow when it is reconnected. Reply,
/// connect and keep-alive waits are never cut below <see cref="ReplyTimeout"/>, so a slow but answering server is not
/// mistaken for a dead one; while nothing was learned, the configured waits apply unchanged. Thread-safe.
/// </summary>
internal sealed class RttEstimator
{
    private readonly Lock _sync = new();
    private long _smoothedTicks;
    private long _deviationTicks;
    private bool _sampled;
    private long _lastSampleAt;

    /// <summary>True once at least one round trip was measured.</summary>
    public bool HasSamples
    {
        get
        {
            lock (_sync)
                return _sampled;
        }
    }

    /// <summary>Smoothed round-trip time (zero while nothing was measured).</summary>
    public TimeSpan Smoothed
    {
        get
        {
            lock (_sync)
                return TimeSpan.FromTicks(_smoothedTicks);
        }
    }

    /// <summary>
    /// The shortest wait for one reply that a reply from this endpoint fits in: the smoothed round trip plus the larger of
    /// four mean deviations and one more smoothed round trip (so at least two round trips). Zero while nothing was
    /// measured.
    /// </summary>
    public TimeSpan ReplyTimeout
    {
        get
        {
            lock (_sync)
            {
                if (!_sampled)
                    return TimeSpan.Zero;
                long margin = Math.Max(_deviationTicks * 4, _smoothedTicks);
                return TimeSpan.FromTicks(_smoothedTicks + margin);
            }
        }
    }

    /// <summary>Time-provider timestamp of the latest sample (0 while nothing was measured): the endpoint last answered then.</summary>
    public long LastSampleAt
    {
        get
        {
            lock (_sync)
                return _lastSampleAt;
        }
    }

    /// <summary>Records one round trip measured at <paramref name="timestamp"/> (a time-provider timestamp).</summary>
    public void Sample(TimeSpan roundTrip, long timestamp)
    {
        long sample = roundTrip.Ticks;
        if (sample < 0)
            return;
        lock (_sync)
        {
            if (timestamp > _lastSampleAt)
                _lastSampleAt = timestamp;
            if (!_sampled)
            {
                _smoothedTicks = sample;
                _deviationTicks = sample / 2;
                _sampled = true;
                return;
            }

            long error = sample - _smoothedTicks;
            _deviationTicks += (Math.Abs(error) - _deviationTicks) / 4;
            _smoothedTicks += error / 8;
        }
    }

    /// <summary>
    /// <paramref name="configured"/> raised to <see cref="ReplyTimeout"/>, but never past <paramref name="limit"/> (the time
    /// the operation has left for the attempt).
    /// </summary>
    public TimeSpan Adapt(TimeSpan configured, TimeSpan limit)
    {
        TimeSpan adaptive = ReplyTimeout;
        TimeSpan wait = adaptive > configured ? adaptive : configured;
        return wait < limit ? wait : limit;
    }
}
