using System.Globalization;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>Object-store access of the process that owns a disk (live node: through its inspector).</summary>
internal interface IObjectStoreAccess
{
    /// <summary>Where the store keeps an object's record and its plaintext length, or null when it does not hold it.</summary>
    Task<(ObjectPhysicalLocation Location, long PlaintextLength)?> LocateAsync(ObjectId id);

    /// <summary>Deletes the local copy without telling anyone; false when it was not held.</summary>
    Task<bool> DeleteLocallyAsync(ObjectId id);
}

/// <summary>The storage medium of a test node, with fault and corruption controls.</summary>
/// <remarks>
/// <see cref="StorageKind.Simulated"/> disks are crash-faithful <see cref="SimulatedMedium"/>s; <see cref="StorageKind.Disk"/>
/// disks are real directories under the cluster's data root, wrapped so that the same fault switches apply and crashes
/// roll the directory back to its durable image. The disk outlives node processes: a restarted node finds exactly what
/// its last crash or stop left.
/// </remarks>
public sealed partial class TestDisk
{
    private readonly object _gate = new();
    private readonly Func<int, IStorageMedium> _createMedium;
    private IStorageMedium _medium;
    private int _generation;

    /// <summary>Creates a disk whose medium generation <c>n</c> comes from <paramref name="createMedium"/> (0 = the first; replacements count up).</summary>
    internal TestDisk(string name, Func<int, IStorageMedium> createMedium)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(createMedium);
        Name = name;
        _createMedium = createMedium;
        _medium = createMedium(0);
    }

    /// <summary>Disk name.</summary>
    public string Name { get; }

    /// <summary>The simulated medium; null for <see cref="StorageKind.Disk"/>.</summary>
    public SimulatedMedium? Simulated => Medium as SimulatedMedium;

    /// <summary>Fault controls (for real disks implemented by a faulting wrapper).</summary>
    public SimulatedMediumFaults Faults => Medium switch
    {
        SimulatedMedium sim => sim.Faults,
        FaultingFileSystemMedium fs => fs.Faults,
        _ => throw new InvalidOperationException("The disk has no fault controls."),
    };

    /// <summary>Root directory for <see cref="StorageKind.Disk"/>; null otherwise.</summary>
    public string? RootDirectory => (Medium as FaultingFileSystemMedium)?.RootDirectory;

    /// <summary>Bytes in use.</summary>
    public long UsedBytes => Medium.UsedBytes;

    /// <summary>The process owning the disk (object-store access for corruption tests).</summary>
    internal IObjectStoreAccess? Owner { get; set; }

    /// <summary>The current physical medium.</summary>
    internal IStorageMedium Medium
    {
        get
        {
            lock (_gate)
                return _medium;
        }
    }

    /// <summary>Fills the disk, leaving <paramref name="leaveFreeBytes"/> free (ENOSPC).</summary>
    public void Fill(long leaveFreeBytes = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(leaveFreeBytes);
        Faults.CapacityOverride = UsedBytes + leaveFreeBytes;
    }

    /// <summary>Makes the disk read-only (or writable again).</summary>
    public void SetReadOnly(bool readOnly) => Faults.ReadOnly = readOnly;

    /// <summary>Flips stored payload bytes of an object on this node.</summary>
    /// <remarks>
    /// <paramref name="offset"/> is a plaintext offset; for an encrypted record the corresponding ciphertext byte of its
    /// sealed unit is flipped (the unit then fails authentication). The node must be running (its store locates the record).
    /// </remarks>
    public async Task CorruptObjectAsync(ObjectId id, long offset = 0, byte xorMask = 0xFF)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (xorMask == 0)
            throw new ArgumentException("A zero mask would not change anything.", nameof(xorMask));
        (ObjectPhysicalLocation location, long plaintext) = await LocateAsync(id);
        if (offset >= plaintext)
            throw new ArgumentOutOfRangeException(nameof(offset), offset, $"Object {id} has {plaintext} plaintext byte(s).");
        long stored;
        if (location.Encoding == 0 || location.UnitSize <= 0)
        {
            stored = location.PayloadOffset + offset;
        }
        else
        {
            long units = (plaintext + location.UnitSize - 1) / location.UnitSize;
            long overhead = units == 0 ? 0 : (location.StoredLength - plaintext) / units;
            long unit = offset / location.UnitSize;
            long within = offset % location.UnitSize;
            long nonce = overhead >= Storage.RecordCipher.NonceSize + Storage.RecordCipher.TagSize ? Storage.RecordCipher.NonceSize : 0;
            stored = location.PayloadOffset + (unit * (location.UnitSize + overhead)) + nonce + within;
        }

        CorruptRaw(location.SegmentPath, stored, xorMask);
    }

    /// <summary>Corrupts an object's identity/length metadata.</summary>
    /// <remarks>Flips a byte of the object id in the record header (its header checksum then fails; the record is unreadable).</remarks>
    public async Task CorruptObjectHeaderAsync(ObjectId id)
    {
        (ObjectPhysicalLocation location, _) = await LocateAsync(id);
        CorruptRaw(location.SegmentPath, location.RecordOffset + 8, 0x5A);
    }

    /// <summary>Silently deletes an object (anti-entropy tests).</summary>
    /// <remarks>The node's store removes its copy without notifying any other node (the node must be running).</remarks>
    public async Task DeleteObjectAsync(ObjectId id)
    {
        IObjectStoreAccess owner = Owner ?? throw new InvalidOperationException($"Disk {Name} belongs to no process that exposes its object store.");
        if (!await owner.DeleteLocallyAsync(id))
            throw new InvalidOperationException($"Disk {Name} does not hold object {id}.");
    }

    /// <summary>All files on the disk.</summary>
    public IReadOnlyList<string> Files() => Medium switch
    {
        SimulatedMedium sim => sim.AllFiles(),
        FaultingFileSystemMedium fs => fs.AllFiles(),
        _ => [],
    };

    /// <summary>Raw durable bytes of a file (encryption-at-rest inspection).</summary>
    public byte[] ReadRaw(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return Medium switch
        {
            SimulatedMedium sim => sim.ReadDurable(path),
            FaultingFileSystemMedium fs => fs.ReadRaw(path),
            _ => throw new InvalidOperationException("The disk cannot be read raw."),
        };
    }

    /// <inheritdoc />
    public override string ToString() => Name;

    /// <summary>Power loss on this disk; returns the acknowledged changes it discarded (dishonest flushes).</summary>
    internal SimulatedLossReport Crash(CrashMode mode)
    {
        switch (Medium)
        {
            case SimulatedMedium sim:
                sim.Crash(mode);
                return sim.LastCrashLoss;
            case FaultingFileSystemMedium fs:
                fs.Crash(mode);
                return fs.LastCrashLoss;
            default:
                return SimulatedLossReport.None;
        }
    }

    /// <summary>Acknowledged changes that are still volatile on this disk.</summary>
    internal SimulatedLossReport AcknowledgedVolatileState() => Medium switch
    {
        SimulatedMedium sim => sim.GetAcknowledgedVolatileState(),
        FaultingFileSystemMedium fs => fs.GetAcknowledgedVolatileState(),
        _ => SimulatedLossReport.None,
    };

    /// <summary>Replaces the disk with an empty one; returns the acknowledged-but-volatile changes that were lost with it.</summary>
    internal SimulatedLossReport Replace()
    {
        SimulatedLossReport lost = AcknowledgedVolatileState();
        Func<int, IStorageMedium> create = _createMedium;
        lock (_gate)
        {
            if (_medium is FaultingFileSystemMedium fs)
            {
                fs.Wipe();
                fs.Faults.Reset();
            }
            else
            {
                _medium = create(++_generation);
            }
        }

        return lost;
    }

    /// <summary>Replaces this disk's content with the durable image of <paramref name="source"/> (a disk clone).</summary>
    internal void CloneFrom(TestDisk source)
    {
        ArgumentNullException.ThrowIfNull(source);
        IStorageMedium from = source.Medium;
        lock (_gate)
        {
            switch (from)
            {
                case SimulatedMedium sim:
                    _medium = sim.CloneDurable(Name + "#clone" + (++_generation).ToString(CultureInfo.InvariantCulture));
                    break;
                case FaultingFileSystemMedium fs when _medium is FaultingFileSystemMedium target:
                    target.CopyFrom(fs);
                    break;
                default:
                    throw new InvalidOperationException($"Disk {source.Name} cannot be cloned onto disk {Name}.");
            }
        }
    }

    private async Task<(ObjectPhysicalLocation Location, long PlaintextLength)> LocateAsync(ObjectId id)
    {
        IObjectStoreAccess owner = Owner ?? throw new InvalidOperationException($"Disk {Name} belongs to no process that exposes its object store.");
        return await owner.LocateAsync(id) ?? throw new InvalidOperationException($"Disk {Name} does not hold object {id}.");
    }

    private void CorruptRaw(string path, long offset, byte xorMask)
    {
        switch (Medium)
        {
            case SimulatedMedium sim:
                sim.CorruptByte(path, offset, xorMask);
                break;
            case FaultingFileSystemMedium fs:
                fs.CorruptByte(path, offset, xorMask);
                break;
            default:
                throw new InvalidOperationException("The disk cannot be corrupted.");
        }
    }
}
