using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>The storage medium of a test node, with fault and corruption controls.</summary>
public sealed partial class TestDisk
{
    internal TestDisk(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Name = name;
    }

    /// <summary>Disk name.</summary>
    public string Name { get; }

    /// <summary>The simulated medium; null for <see cref="StorageKind.Disk"/>.</summary>
    public SimulatedMedium? Simulated => throw Stub.NotYet("TestDisk.Simulated");

    /// <summary>Fault controls (for real disks implemented by a faulting wrapper).</summary>
    public SimulatedMediumFaults Faults => throw Stub.NotYet("TestDisk.Faults");

    /// <summary>Root directory for <see cref="StorageKind.Disk"/>; null otherwise.</summary>
    public string? RootDirectory => throw Stub.NotYet("TestDisk.RootDirectory");

    /// <summary>Bytes in use.</summary>
    public long UsedBytes => throw Stub.NotYet("TestDisk.UsedBytes");

    /// <summary>Fills the disk, leaving <paramref name="leaveFreeBytes"/> free (ENOSPC).</summary>
    public void Fill(long leaveFreeBytes = 0) => throw Stub.NotYet("TestDisk.Fill");

    /// <summary>Makes the disk read-only (or writable again).</summary>
    public void SetReadOnly(bool readOnly) => throw Stub.NotYet("TestDisk.SetReadOnly");

    /// <summary>Flips stored payload bytes of an object on this node.</summary>
    public Task CorruptObjectAsync(ObjectId id, long offset = 0, byte xorMask = 0xFF) => throw Stub.NotYet("TestDisk.CorruptObjectAsync");

    /// <summary>Corrupts an object's identity/length metadata.</summary>
    public Task CorruptObjectHeaderAsync(ObjectId id) => throw Stub.NotYet("TestDisk.CorruptObjectHeaderAsync");

    /// <summary>Silently deletes an object (anti-entropy tests).</summary>
    public Task DeleteObjectAsync(ObjectId id) => throw Stub.NotYet("TestDisk.DeleteObjectAsync");

    /// <summary>All files on the disk.</summary>
    public IReadOnlyList<string> Files() => throw Stub.NotYet("TestDisk.Files");

    /// <summary>Raw durable bytes of a file (encryption-at-rest inspection).</summary>
    public byte[] ReadRaw(string path) => throw Stub.NotYet("TestDisk.ReadRaw");

    /// <inheritdoc />
    public override string ToString() => Name;
}
