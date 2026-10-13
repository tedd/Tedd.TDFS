using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Token bucket for background byte work (repair copies, scrubbing) on the node loop. Tokens accrue at
/// <see cref="BytesPerSecond"/> up to one second's worth; a caller may overdraw (one object never waits for more than its
/// own size) and then waits on the node clock until the debt is paid, so the long-run rate never exceeds the limit.
/// </summary>
internal sealed class ByteRateLimiter
{
    private readonly NodeServices _s;
    private double _tokens;
    private long _stamp;

    public ByteRateLimiter(NodeServices services, long bytesPerSecond)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentOutOfRangeException.ThrowIfLessThan(bytesPerSecond, 1);
        _s = services;
        BytesPerSecond = bytesPerSecond;
        _tokens = bytesPerSecond;
        _stamp = services.Time.GetTimestamp();
    }

    /// <summary>Sustained rate limit.</summary>
    public long BytesPerSecond { get; }

    /// <summary>Tokens available now (bytes that may be taken without waiting).</summary>
    public long Available
    {
        get
        {
            Refill();
            return (long)Math.Max(0, _tokens);
        }
    }

    /// <summary>Takes <paramref name="bytes"/>, waiting until the bucket's debt is paid.</summary>
    public async Task TakeAsync(long bytes, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bytes);
        Refill();
        _tokens -= bytes;
        if (_tokens >= 0)
            return;
        TimeSpan wait = TimeSpan.FromSeconds(-_tokens / BytesPerSecond);
        await Task.Delay(wait, _s.Time, ct);
    }

    /// <summary>Charges work already done (the bucket may go into debt; <see cref="Available"/> stays 0 until it is paid).</summary>
    public void Charge(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bytes);
        Refill();
        _tokens -= bytes;
    }

    /// <summary>Takes up to <paramref name="bytes"/> without waiting; returns what was granted (0 when the bucket is empty).</summary>
    public long TryTake(long bytes)
    {
        Refill();
        long granted = (long)Math.Clamp(_tokens, 0, bytes);
        _tokens -= granted;
        return granted;
    }

    private void Refill()
    {
        long now = _s.Time.GetTimestamp();
        double elapsed = _s.Time.GetElapsedTime(_stamp, now).TotalSeconds;
        _stamp = now;
        if (elapsed > 0)
            _tokens = Math.Min(BytesPerSecond, _tokens + (elapsed * BytesPerSecond));
    }
}
