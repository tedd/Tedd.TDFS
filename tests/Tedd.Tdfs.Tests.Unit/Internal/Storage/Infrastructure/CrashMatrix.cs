using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>Describes one crash case of a <see cref="CrashMatrix"/> run.</summary>
public sealed record CrashCase(CrashMode Mode, ulong Seed, int Step, bool Completed, string? CrashedAt)
{
    public override string ToString()
        => $"mode={Mode} seed={Seed} step={Step} completed={Completed} at='{CrashedAt ?? "(after completion)"}'";
}

/// <summary>
/// Runs an operation against a <see cref="SimulatedMedium"/> and crashes it before every mutating I/O step in turn (and
/// once after completion), with every <see cref="CrashMode"/> (several seeds for the random modes). After each crash the
/// verifier reopens the component and checks that the recovered state is allowed; the verifier then runs again after a
/// second crash (<see cref="CrashMode.LoseUnflushed"/>) and must report the same state, proving recovery itself is durable.
/// </summary>
public static class CrashMatrix
{
    public const long Capacity = 1L << 34;

    public static IReadOnlyList<CrashMode> Modes { get; } = Enum.GetValues<CrashMode>();

    /// <param name="prepare">Builds the durable initial state on a fresh medium.</param>
    /// <param name="operation">The operation under test; runs against the crashing medium.</param>
    /// <param name="verify">Reopens the component on the crashed medium, asserts the state is allowed and returns a fingerprint of it.</param>
    /// <param name="seedsPerRandomMode">Seeds for <see cref="CrashMode.RandomSubset"/> and <see cref="CrashMode.TearLastWrite"/>.</param>
    /// <returns>The number of crash cases executed.</returns>
    public static async Task<int> RunAsync(
        Func<SimulatedMedium, Task> prepare,
        Func<IStorageMedium, Task> operation,
        Func<SimulatedMedium, CrashCase, Task<string>> verify,
        int seedsPerRandomMode = 3,
        int maxSteps = 20_000)
    {
        int cases = 0;
        foreach (CrashMode mode in Modes)
        {
            int seeds = mode is CrashMode.RandomSubset or CrashMode.TearLastWrite ? seedsPerRandomMode : 1;
            for (ulong seed = 1; seed <= (ulong)seeds; seed++)
            {
                for (int step = 0; ; step++)
                {
                    if (step > maxSteps)
                        throw new InvalidOperationException($"The operation did not complete within {maxSteps} steps.");
                    var medium = new SimulatedMedium("crash", Capacity, new SeededRandom((seed * 1_000_003) + (ulong)step));
                    await prepare(medium);

                    var crashing = new CrashingMedium(medium, step, mode);
                    bool completed = false;
                    try
                    {
                        await operation(crashing);
                        completed = true;
                    }
                    catch (Exception) when (crashing.Crashed)
                    {
                        // The process died at this step (the component may surface the crash in any form).
                    }

                    if (completed)
                        medium.Crash(mode);
                    var crashCase = new CrashCase(mode, seed, step, completed, crashing.CrashedAt);
                    try
                    {
                        string first = await verify(medium, crashCase);
                        medium.Crash(CrashMode.LoseUnflushed);
                        string second = await verify(medium, crashCase);
                        Assert.Equal(first, second);
                    }
                    catch (Exception ex)
                    {
                        throw new CrashCaseFailedException($"Crash case failed: {crashCase}: {ex.Message}", ex);
                    }

                    cases++;
                    if (completed)
                        break;
                }
            }
        }

        return cases;
    }
}

/// <summary>Wraps a verification failure with the crash case that produced it.</summary>
public sealed class CrashCaseFailedException : Exception
{
    public CrashCaseFailedException()
    {
    }

    public CrashCaseFailedException(string message)
        : base(message)
    {
    }

    public CrashCaseFailedException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
