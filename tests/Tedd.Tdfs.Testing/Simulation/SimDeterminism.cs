using System.Globalization;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Everything observable about one simulation run that must replay exactly from its seed (HAR-001).</summary>
/// <param name="Seed">The master seed.</param>
/// <param name="Trace">The scheduler trace (events, and steps when recorded).</param>
/// <param name="ScheduleFingerprint">Hash of the executed interleaving.</param>
/// <param name="Steps">Number of executed work items.</param>
/// <param name="VirtualTime">Virtual time at the end of the run.</param>
/// <param name="Digest">Scenario-defined digest of final state and outcomes.</param>
public sealed record SimRunRecord(ulong Seed, IReadOnlyList<string> Trace, ulong ScheduleFingerprint, long Steps, TimeSpan VirtualTime, string Digest)
{
    /// <summary>Captures the current state of <paramref name="scheduler"/>.</summary>
    public static SimRunRecord Capture(SimScheduler scheduler, string digest = "")
    {
        ArgumentNullException.ThrowIfNull(scheduler);
        ArgumentNullException.ThrowIfNull(digest);
        return new SimRunRecord(scheduler.Seed, scheduler.Trace, scheduler.ScheduleFingerprint, scheduler.StepCount, scheduler.Now, digest);
    }

    /// <summary>
    /// Describes the first observable difference from <paramref name="other"/> (ignoring <see cref="Seed"/>), or returns
    /// null when the runs are indistinguishable.
    /// </summary>
    public string? FirstDifference(SimRunRecord other)
    {
        ArgumentNullException.ThrowIfNull(other);
        int common = Math.Min(Trace.Count, other.Trace.Count);
        for (int i = 0; i < common; i++)
        {
            if (!string.Equals(Trace[i], other.Trace[i], StringComparison.Ordinal))
                return string.Create(CultureInfo.InvariantCulture, $"trace line {i} differs: '{Trace[i]}' vs '{other.Trace[i]}'");
        }

        if (Trace.Count != other.Trace.Count)
        {
            string extra = Trace.Count > other.Trace.Count ? Trace[common] : other.Trace[common];
            return string.Create(CultureInfo.InvariantCulture, $"trace lengths differ ({Trace.Count} vs {other.Trace.Count}); first extra line: '{extra}'");
        }

        if (ScheduleFingerprint != other.ScheduleFingerprint)
            return string.Create(CultureInfo.InvariantCulture, $"schedule fingerprints differ ({ScheduleFingerprint:X16} vs {other.ScheduleFingerprint:X16})");
        if (Steps != other.Steps)
            return string.Create(CultureInfo.InvariantCulture, $"step counts differ ({Steps} vs {other.Steps})");
        if (VirtualTime != other.VirtualTime)
            return string.Create(CultureInfo.InvariantCulture, $"final virtual times differ ({SimScheduler.FormatTime(VirtualTime)} vs {SimScheduler.FormatTime(other.VirtualTime)})");
        if (!string.Equals(Digest, other.Digest, StringComparison.Ordinal))
            return $"digests differ ('{Digest}' vs '{other.Digest}')";
        return null;
    }
}

/// <summary>Reusable determinism self-tests over any scenario that maps a seed to a <see cref="SimRunRecord"/>.</summary>
public static class SimDeterminism
{
    /// <summary>
    /// Runs <paramref name="scenario"/> <paramref name="runs"/> times with <paramref name="seed"/> and fails (xunit) unless every
    /// run is indistinguishable from the first (HAR-001). Returns the first run's record.
    /// </summary>
    public static SimRunRecord AssertReplayIsExact(Func<ulong, SimRunRecord> scenario, ulong seed, int runs = 2)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        ArgumentOutOfRangeException.ThrowIfLessThan(runs, 2);
        SimRunRecord first = scenario(seed);
        for (int run = 2; run <= runs; run++)
        {
            SimRunRecord again = scenario(seed);
            string? difference = first.FirstDifference(again);
            if (difference is not null)
                Assert.Fail(string.Create(CultureInfo.InvariantCulture, $"Replay of seed {seed} diverged on run {run}: {difference}"));
        }

        return first;
    }

    /// <summary>
    /// Runs <paramref name="scenario"/> once per seed and fails (xunit) unless every pair of seeds produced a different
    /// interleaving (schedule fingerprint) and a different trace. Returns the records in seed order.
    /// </summary>
    public static IReadOnlyList<SimRunRecord> AssertSeedsDiverge(Func<ulong, SimRunRecord> scenario, params ulong[] seeds)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        ArgumentNullException.ThrowIfNull(seeds);
        if (seeds.Length < 2)
            throw new ArgumentException("At least two seeds are required.", nameof(seeds));
        if (seeds.Distinct().Count() != seeds.Length)
            throw new ArgumentException("Seeds must be distinct.", nameof(seeds));

        var records = new SimRunRecord[seeds.Length];
        for (int i = 0; i < seeds.Length; i++)
            records[i] = scenario(seeds[i]);
        for (int i = 0; i < records.Length; i++)
        {
            for (int j = i + 1; j < records.Length; j++)
            {
                if (records[i].ScheduleFingerprint == records[j].ScheduleFingerprint)
                    Assert.Fail(string.Create(CultureInfo.InvariantCulture, $"Seeds {seeds[i]} and {seeds[j]} produced the same interleaving (fingerprint {records[i].ScheduleFingerprint:X16})."));
                if (records[i].Trace.SequenceEqual(records[j].Trace, StringComparer.Ordinal))
                    Assert.Fail(string.Create(CultureInfo.InvariantCulture, $"Seeds {seeds[i]} and {seeds[j]} produced identical traces."));
            }
        }

        return records;
    }
}
