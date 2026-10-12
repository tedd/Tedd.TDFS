namespace Tedd.Tdfs.Server;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): disk-format introspection.
public sealed partial class NodeInspector
{
    /// <summary>
    /// Reads the format versions of this node's durable image (superblock, journal segments, newest checkpoint, object
    /// segments) and its downgrade-protection marker, on the node loop. Reports what is durably recorded on the medium, not
    /// in-memory intent: a migration in progress that has not yet been made durable is not reflected.
    /// </summary>
    public Task<DiskFormatInspection> GetDiskFormatAsync()
        => throw new NotImplementedException("NodeInspector.GetDiskFormatAsync: implemented in a later phase");
}
