using System.Buffers.Binary;
using System.Globalization;
using Tedd.Tdfs.Data;

namespace Tedd.Tdfs.Storage;

/// <summary>Shared on-disk format helpers: magic numbers, little-endian field access and CRC framing.</summary>
internal static class StorageFormat
{
    /// <summary>"TDAF": <see cref="AtomicFile"/> frame.</summary>
    public const uint AtomicFileMagic = 0x46414454;

    /// <summary>"TDJS": journal segment header.</summary>
    public const uint JournalSegmentMagic = 0x534A4454;

    /// <summary>"TJRL": journal record header.</summary>
    public const uint JournalRecordMagic = 0x4C524A54;

    /// <summary>"TOSG": object segment header.</summary>
    public const uint ObjectSegmentMagic = 0x47534F54;

    /// <summary>"TOBR": object record header.</summary>
    public const uint ObjectRecordMagic = 0x52424F54;

    /// <summary>"TCKP": checkpoint file header.</summary>
    public const uint CheckpointMagic = 0x504B4354;

    /// <summary>"TCKC": checkpoint chunk.</summary>
    public const uint CheckpointChunkMagic = 0x434B4354;

    /// <summary>"TCKE": checkpoint trailer.</summary>
    public const uint CheckpointTrailerMagic = 0x454B4354;

    /// <summary>"TDSB": node superblock payload.</summary>
    public const uint SuperblockMagic = 0x42534454;

    /// <summary>Computes the CRC-32C of <paramref name="data"/>.</summary>
    public static uint Crc(ReadOnlySpan<byte> data) => ObjectChecksum.Crc32C(data);

    /// <summary>Writes the CRC-32C of <c>buffer[..crcOffset]</c> at <paramref name="crcOffset"/>.</summary>
    public static void SealCrc(Span<byte> buffer, int crcOffset)
        => BinaryPrimitives.WriteUInt32LittleEndian(buffer[crcOffset..], Crc(buffer[..crcOffset]));

    /// <summary>True when the CRC-32C stored at <paramref name="crcOffset"/> matches <c>buffer[..crcOffset]</c>.</summary>
    public static bool CheckCrc(ReadOnlySpan<byte> buffer, int crcOffset)
        => buffer.Length >= crcOffset + 4 && BinaryPrimitives.ReadUInt32LittleEndian(buffer[crcOffset..]) == Crc(buffer[..crcOffset]);

    /// <summary>Formats a 64-bit value as 16 lowercase hex digits (file names sort in numeric order).</summary>
    public static string Hex16(ulong value) => value.ToString("x16", CultureInfo.InvariantCulture);

    /// <summary>Formats a 32-bit value as 8 lowercase hex digits.</summary>
    public static string Hex8(uint value) => value.ToString("x8", CultureInfo.InvariantCulture);

    /// <summary>Parses exactly 16 lowercase or uppercase hex digits.</summary>
    public static bool TryParseHex16(ReadOnlySpan<char> text, out ulong value)
    {
        value = 0;
        return text.Length == 16 && IsHex(text) && ulong.TryParse(text, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out value);
    }

    /// <summary>Parses exactly 8 hex digits.</summary>
    public static bool TryParseHex8(ReadOnlySpan<char> text, out uint value)
    {
        value = 0;
        return text.Length == 8 && IsHex(text) && uint.TryParse(text, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out value);
    }

    private static bool IsHex(ReadOnlySpan<char> text)
    {
        foreach (char c in text)
        {
            if (!char.IsAsciiHexDigit(c))
                return false;
        }

        return true;
    }

    /// <summary>Reads a whole file into memory.</summary>
    /// <exception cref="IOException">The file is too large to hold in memory.</exception>
    public static async ValueTask<byte[]> ReadAllAsync(IStorageFile file, CancellationToken ct)
    {
        long length = file.Length;
        if (length > Array.MaxLength)
            throw new IOException($"File '{file.Path}' is too large to read into memory ({length} bytes).");
        var data = new byte[length];
        int read = 0;
        while (read < data.Length)
        {
            int n = await file.ReadAsync(read, data.AsMemory(read), ct);
            if (n <= 0)
                break; // the file shrank concurrently; return what exists
            read += n;
        }

        return read == data.Length ? data : data.AsSpan(0, read).ToArray();
    }

    /// <summary>Closes a file, swallowing errors (best-effort cleanup paths only).</summary>
    public static async ValueTask CloseQuietlyAsync(IStorageFile? file)
    {
        if (file is null)
            return;
        try
        {
            await file.DisposeAsync();
        }
        catch (IOException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }

    /// <summary>True for exceptions that best-effort cleanup may swallow (device errors and handles invalidated by a crash).</summary>
    public static bool IsCleanupTolerable(Exception ex) => ex is IOException or ObjectDisposedException or UnauthorizedAccessException;

    /// <summary>Wraps a disk-full error into a <see cref="DfsCapacityException"/>; other exceptions are returned unchanged.</summary>
    public static Exception MapCapacity(Exception ex, string what)
        => StorageErrors.IsDiskFull(ex) && ex is not DfsCapacityException
            ? new DfsCapacityException($"Storage is full while {what}.", ex)
            : ex;
}
