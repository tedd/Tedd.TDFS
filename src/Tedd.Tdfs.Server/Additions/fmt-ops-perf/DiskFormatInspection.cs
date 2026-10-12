namespace Tedd.Tdfs.Server;

/// <summary>
/// Format versions of a live node's durable image, as read from the medium (FMT-005, FMT-006, FMT-008).
/// </summary>
/// <param name="SuperblockVersion">Format version recorded in the node superblock.</param>
/// <param name="JournalFormatVersion">Highest format version among the node's Raft journal segments.</param>
/// <param name="CheckpointFormatVersion">Format version of the newest durable metadata checkpoint (0 when none exists yet).</param>
/// <param name="ObjectSegmentFormatVersion">Highest format version among the node's object segments (0 when none exist).</param>
/// <param name="MinimumReaderVersion">
/// The lowest disk-format version a release must be able to read to open this image; raised durably by irreversible
/// format/feature activations and never lowered (downgrade protection).
/// </param>
/// <param name="ActivatedIrreversibleFeatures">Irreversible features recorded as activated on this image, sorted ordinally.</param>
public sealed record DiskFormatInspection(
    int SuperblockVersion,
    int JournalFormatVersion,
    int CheckpointFormatVersion,
    int ObjectSegmentFormatVersion,
    int MinimumReaderVersion,
    IReadOnlyList<string> ActivatedIrreversibleFeatures)
{
    /// <summary>The highest format version of any component that exists on the image.</summary>
    public int HighestComponentVersion =>
        Math.Max(Math.Max(SuperblockVersion, JournalFormatVersion), Math.Max(CheckpointFormatVersion, ObjectSegmentFormatVersion));
}
