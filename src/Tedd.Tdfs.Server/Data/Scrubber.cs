using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Background scrubbing of the local object store (DSK-016, RSY-025): every <see cref="NodeTimings.ScrubInterval"/> a pass
/// re-verifies every stored object (durable record header, every unit, the whole-object checksum), rate-limited so it
/// never competes with foreground work; <c>AdminOp.StartScrub</c> requests an immediate, faster pass. A corrupt copy is
/// quarantined durably (it leaves the index and is never served or counted again), counted as a checksum failure, and an
/// inventory report follows promptly so the volume leader stops counting it and repairs the replica from a verified
/// copy. Runs on the node loop.
/// </summary>
internal sealed class Scrubber
{
    private readonly NodeServices _s;
    private readonly ByteRateLimiter _background;
    private readonly ByteRateLimiter _requestedRate;
    private bool _passActive;
    private bool _requestedPass;
    private bool _requested;
    private int _passTarget;
    private int _passChecked;
    private long _lastPassEnd;
    private bool _running;

    public Scrubber(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
        long repair = services.Timings.RepairBytesPerSecond;
        _background = new ByteRateLimiter(services, Math.Max(1, repair / 8));
        _requestedRate = new ByteRateLimiter(services, Math.Max(1, repair / 2));
        _lastPassEnd = services.Time.GetTimestamp();
    }

    /// <summary>Interval of the scrub job's steps.</summary>
    public TimeSpan Step => TimeSpan.FromTicks(Math.Clamp(_s.Timings.ScrubInterval.Ticks / 8, TimeSpan.FromMilliseconds(50).Ticks, TimeSpan.FromSeconds(30).Ticks));

    /// <summary>Requests a full pass now (<c>AdminOp.StartScrub</c>); a pass in progress restarts so it covers everything stored now.</summary>
    public void RequestFullPass() => _requested = true;

    /// <summary>One step of the scrubber (periodic job).</summary>
    public async Task RunAsync()
    {
        if (_running || !_s.IsServing || _s.IsStopping)
            return;
        _running = true;
        try
        {
            await StepAsync();
        }
        finally
        {
            _running = false;
        }
    }

    private async Task StepAsync()
    {
        ObjectStore store = _s.ObjectStore;
        long now = _s.Time.GetTimestamp();
        if (_requested)
        {
            _requested = false;
            _requestedPass = true;
            StartPass(store);
        }
        else if (!_passActive)
        {
            if (_s.Time.GetElapsedTime(_lastPassEnd, now) < _s.Timings.ScrubInterval)
                return;
            _requestedPass = false;
            StartPass(store);
        }

        ByteRateLimiter rate = _requestedPass ? _requestedRate : _background;
        int corrupt = 0;
        while (_passActive && _passChecked < _passTarget && rate.Available > 0 && !_s.IsStopping)
        {
            ScrubResult result = await store.ScrubAsync(1);
            _passChecked++;
            rate.Charge(result.CheckedBytes);
            corrupt += result.Corrupt.Count;
            if (_requested)
                break;
        }

        if (corrupt > 0)
        {
            for (int i = 0; i < corrupt; i++)
                _s.Metrics.ChecksumFailure();
            _s.Metrics.SetStored(store.ObjectCount, store.LiveBytes);
            _s.Warn("scrub", NodeServices.Inv($"Scrubbing quarantined {corrupt} corrupt object cop{(corrupt == 1 ? "y" : "ies")} on {_s.NodeName}."));
            _s.Maintenance?.Inventory.RequestReport();
        }

        if (_passActive && _passChecked >= _passTarget)
        {
            _passActive = false;
            _requestedPass = false;
            _lastPassEnd = _s.Time.GetTimestamp();
        }
    }

    private void StartPass(ObjectStore store)
    {
        _passActive = true;
        _passChecked = 0;
        _passTarget = store.ObjectCount;
    }
}
