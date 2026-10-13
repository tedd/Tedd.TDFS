using System.Buffers.Binary;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): disk-format introspection.
public sealed partial class NodeInspector
{
    /// <summary>
    /// Reads the format versions of this node's durable image (superblock, journal segments, newest checkpoint, object
    /// segments) and its downgrade-protection marker, on the node loop. Reports what is durably recorded on the medium, not
    /// in-memory intent: a migration in progress that has not yet been made durable is not reflected.
    /// </summary>
    /// <remarks>
    /// The superblock is decoded from the medium; journal segments (<c>*.jseg</c>), checkpoints (<c>*.ckpt</c>) and object
    /// segments (<c>*.oseg</c>) anywhere below the data directory are identified by their header magic and their header
    /// version is read (the highest per component is reported; 0 when the component has no file yet). The minimum reader
    /// version is the superblock's durable minimum software version; the activated irreversible features are those of the
    /// applied control state (a committed, durable log prefix) that the build classifies as irreversible.
    /// </remarks>
    public Task<DiskFormatInspection> GetDiskFormatAsync() => _loop.InvokeAsync(async () =>
    {
        IStorageMedium medium = S.Env.Medium;
        Superblock? superblock = await Superblock.LoadAsync(medium, S.DataPath(Superblock.DefaultFileName));
        var versions = new FormatScan();
        await ScanAsync(medium, S.Options.DataDirectory, versions, depth: 0);
        string[] irreversible = S.Control.State.Features
            .Where(ProtocolFeatures.Irreversible.Contains)
            .Order(StringComparer.Ordinal)
            .ToArray();
        return new DiskFormatInspection(
            superblock?.FormatVersion ?? 0,
            versions.Journal,
            versions.Checkpoint,
            versions.ObjectSegment,
            superblock?.MinSoftwareVersion ?? 0,
            irreversible);
    });

    private static async Task ScanAsync(IStorageMedium medium, string directory, FormatScan versions, int depth)
    {
        if (depth > 16)
            return;
        IReadOnlyList<string> entries;
        try
        {
            entries = await medium.ListAsync(directory);
        }
        catch (Exception ex) when (ex is DirectoryNotFoundException or IOException)
        {
            return;
        }

        foreach (string name in entries)
        {
            string path = StoragePath.Combine(directory, name);
            if (name.EndsWith(Journal.SegmentExtension, StringComparison.Ordinal))
                versions.Journal = Math.Max(versions.Journal, await ReadHeaderVersionAsync(medium, path, StorageFormat.JournalSegmentMagic));
            else if (name.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal))
                versions.Checkpoint = Math.Max(versions.Checkpoint, await ReadHeaderVersionAsync(medium, path, StorageFormat.CheckpointMagic));
            else if (name.EndsWith(ObjectStore.SegmentExtension, StringComparison.Ordinal))
                versions.ObjectSegment = Math.Max(versions.ObjectSegment, await ReadHeaderVersionAsync(medium, path, StorageFormat.ObjectSegmentMagic));
            else if (!name.Contains('.', StringComparison.Ordinal))
                await ScanAsync(medium, path, versions, depth + 1);
        }
    }

    /// <summary>The version in a component header (<c>u32 magic | u16 version</c>), or 0 when the file is not such a component.</summary>
    private static async Task<int> ReadHeaderVersionAsync(IStorageMedium medium, string path, uint magic)
    {
        try
        {
            await using IStorageFile file = await medium.OpenAsync(path, StorageOpenMode.OpenExisting);
            byte[] header = new byte[6];
            int read = 0;
            while (read < header.Length)
            {
                int n = await file.ReadAsync(read, header.AsMemory(read));
                if (n <= 0)
                    return 0;
                read += n;
            }

            return BinaryPrimitives.ReadUInt32LittleEndian(header) == magic ? BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(4)) : 0;
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException or IOException or UnauthorizedAccessException)
        {
            return 0;
        }
    }

    private sealed class FormatScan
    {
        public int Journal { get; set; }

        public int Checkpoint { get; set; }

        public int ObjectSegment { get; set; }
    }
}
