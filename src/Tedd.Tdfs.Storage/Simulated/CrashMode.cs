namespace Tedd.Tdfs.Storage;

/// <summary>What survives a simulated crash of a <see cref="SimulatedMedium"/>.</summary>
public enum CrashMode
{
    /// <summary>Every unflushed file write and every unflushed namespace change is lost.</summary>
    LoseUnflushed,

    /// <summary>Every unflushed file write and namespace change survives (as if flushed just before the crash).</summary>
    KeepUnflushed,

    /// <summary>
    /// Seeded mix: each unflushed file write or length change independently survives or is lost (survivors are applied
    /// in their original order); a random prefix of the ordered unflushed namespace changes survives.
    /// </summary>
    RandomSubset,

    /// <summary>
    /// Everything unflushed survives except the most recent unflushed write on the medium, which is torn: only a random
    /// strict prefix of its bytes reaches the device.
    /// </summary>
    TearLastWrite,
}
