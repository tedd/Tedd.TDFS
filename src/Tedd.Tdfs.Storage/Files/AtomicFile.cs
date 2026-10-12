using System.Buffers.Binary;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// Atomic replacement of small files: write temp, flush, rename over the target, flush the directory.
/// </summary>
/// <remarks>
/// <para>On-disk frame (little-endian): <c>u32 magic "TDAF" | u16 version | u16 flags (0) | u64 contentLength |
/// content | u32 CRC-32C over everything before it</c>.</para>
/// <para>A crash at any point leaves either the complete previous content or the complete new content at the target
/// path. The temporary file (<c>path + </c><see cref="TempSuffix"/>) is never read; a stale one left by a crash is
/// overwritten by the next write. Concurrent writes to the same path must be serialized by the caller.</para>
/// </remarks>
public static class AtomicFile
{
    /// <summary>Suffix of the temporary file used while writing.</summary>
    public const string TempSuffix = ".tmp";

    /// <summary>Bytes added to the content by the frame (header + CRC trailer).</summary>
    public const int FrameOverhead = HeaderSize + 4;

    /// <summary>Frame format version written by this implementation.</summary>
    public const ushort FormatVersion = 1;

    private const int HeaderSize = 16;

    /// <summary>Atomically and durably replaces <paramref name="path"/> with <paramref name="content"/>.</summary>
    /// <remarks>Creates the parent directory (durably) when it does not exist. Durable on return.</remarks>
    public static async ValueTask WriteAsync(IStorageMedium medium, string path, ReadOnlyMemory<byte> content, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        string target = NormalizeFilePath(path);
        string parent = StoragePath.GetParent(target);
        if (parent.Length != 0 && !await medium.ExistsAsync(parent, ct))
            await medium.CreateDirectoryDurableAsync(parent, ct);

        byte[] frame = Encode(content.Span);
        string temp = target + TempSuffix;
        IStorageFile file = await medium.OpenAsync(temp, StorageOpenMode.Truncate, ct);
        try
        {
            await file.WriteFullyAsync(0, frame, ct);
            await file.FlushAsync(ct);
        }
        finally
        {
            await StorageFormat.CloseQuietlyAsync(file);
        }

        await medium.RenameAsync(temp, target, overwrite: true, ct);
        await medium.FlushDirectoryAsync(parent, ct);
    }

    /// <summary>Reads a file written by <see cref="WriteAsync"/>; null when absent.</summary>
    /// <exception cref="DfsIntegrityException">The file is torn or corrupt.</exception>
    /// <exception cref="DfsProtocolException">The file uses a newer frame format (<see cref="DfsErrorCode.Unsupported"/>).</exception>
    public static async ValueTask<byte[]?> ReadAsync(IStorageMedium medium, string path, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        string target = NormalizeFilePath(path);
        byte[]? raw = await medium.TryReadAllBytesAsync(target, ct);
        return raw is null ? null : Decode(raw, target);
    }

    /// <summary>Durably deletes <paramref name="path"/> and any stale temporary file next to it (no-op when absent).</summary>
    public static async ValueTask DeleteAsync(IStorageMedium medium, string path, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        string target = NormalizeFilePath(path);
        string parent = StoragePath.GetParent(target);
        bool changed = false;
        foreach (string p in new[] { target, target + TempSuffix })
        {
            if (await medium.ExistsAsync(p, ct))
            {
                await medium.DeleteAsync(p, ct);
                changed = true;
            }
        }

        if (changed)
            await medium.FlushDirectoryAsync(parent, ct);
    }

    /// <summary>True when <paramref name="name"/> is the name of an <see cref="AtomicFile"/> temporary file.</summary>
    public static bool IsTempName(string name) => name is not null && name.EndsWith(TempSuffix, StringComparison.Ordinal);

    /// <summary>Frames <paramref name="content"/> (header + content + CRC).</summary>
    internal static byte[] Encode(ReadOnlySpan<byte> content)
    {
        var frame = new byte[HeaderSize + content.Length + 4];
        Span<byte> s = frame;
        BinaryPrimitives.WriteUInt32LittleEndian(s, StorageFormat.AtomicFileMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(s[4..], FormatVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(s[6..], 0);
        BinaryPrimitives.WriteUInt64LittleEndian(s[8..], (ulong)content.Length);
        content.CopyTo(s[HeaderSize..]);
        StorageFormat.SealCrc(s, HeaderSize + content.Length);
        return frame;
    }

    /// <summary>Validates a frame and returns its content.</summary>
    internal static byte[] Decode(ReadOnlySpan<byte> raw, string path)
    {
        if (raw.Length < FrameOverhead)
            throw new DfsIntegrityException($"Atomic file '{path}' is torn: {raw.Length} bytes is shorter than the {FrameOverhead}-byte frame.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(raw) != StorageFormat.AtomicFileMagic)
            throw new DfsIntegrityException($"Atomic file '{path}' has an invalid magic number.");
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(raw[4..]);
        ulong length = BinaryPrimitives.ReadUInt64LittleEndian(raw[8..]);
        if (length != (ulong)(raw.Length - FrameOverhead))
            throw new DfsIntegrityException($"Atomic file '{path}' is torn or corrupt: declared {length} content bytes, found {raw.Length - FrameOverhead}.");
        if (!StorageFormat.CheckCrc(raw, raw.Length - 4))
            throw new DfsIntegrityException($"Atomic file '{path}' failed its CRC check.");
        if (version != FormatVersion)
        {
            throw new DfsProtocolException(DfsErrorCode.Unsupported,
                $"Atomic file '{path}' uses frame format version {version}; this software supports version {FormatVersion}.");
        }

        if (BinaryPrimitives.ReadUInt16LittleEndian(raw[6..]) != 0)
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Atomic file '{path}' uses unknown frame flags.");
        return raw.Slice(HeaderSize, (int)length).ToArray();
    }

    private static string NormalizeFilePath(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        string normalized = StoragePath.Normalize(path);
        if (normalized.Length == 0)
            throw new ArgumentException("A file path is required (the root directory is not a file).", nameof(path));
        return normalized;
    }
}
