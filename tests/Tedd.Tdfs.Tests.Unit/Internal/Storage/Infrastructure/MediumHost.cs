using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>A storage medium for contract tests that can be "restarted" (same durable data, fresh process state).</summary>
public abstract class MediumHost : IAsyncDisposable
{
    public abstract IStorageMedium Medium { get; }

    /// <summary>True for <see cref="SimulatedMedium"/>-backed hosts (fault and crash controls available).</summary>
    public abstract bool IsSimulated { get; }

    /// <summary>The simulated medium (null for a real file system).</summary>
    public virtual SimulatedMedium? Simulated => null;

    /// <summary>Simulates a clean process restart: callers must have disposed their components first.</summary>
    public abstract IStorageMedium Restart();

    /// <summary>Reads a file's bytes as they are stored (bypassing every component).</summary>
    public abstract byte[] ReadRaw(string path);

    /// <summary>Overwrites stored bytes in place (corruption injection).</summary>
    public abstract void Corrupt(string path, long offset, byte xorMask = 0xFF);

    public abstract ValueTask DisposeAsync();
}

/// <summary>In-memory crash-faithful medium.</summary>
public sealed class SimulatedHost : MediumHost
{
    private readonly SimulatedMedium _medium;

    public SimulatedHost(ulong seed = 42, long capacity = 1L << 34)
    {
        _medium = new SimulatedMedium("sim", capacity, new SeededRandom(seed));
    }

    public override IStorageMedium Medium => _medium;

    public override bool IsSimulated => true;

    public override SimulatedMedium Simulated => _medium;

    public override IStorageMedium Restart() => _medium;

    public override byte[] ReadRaw(string path) => _medium.ReadCurrent(path);

    public override void Corrupt(string path, long offset, byte xorMask = 0xFF) => _medium.CorruptByte(path, offset, xorMask);

    public override ValueTask DisposeAsync() => default;
}

/// <summary>Real files in a fresh directory under the test root (env TDFS_TEST_ROOT, default D:\Workspaces\AI\tdfs-tests).</summary>
public sealed class FileSystemHost : MediumHost
{
    private FileSystemMedium _medium;

    public FileSystemHost()
    {
        string root = Environment.GetEnvironmentVariable("TDFS_TEST_ROOT") is { Length: > 0 } configured ? configured : @"D:\Workspaces\AI\tdfs-tests";
        RootDirectory = Path.Combine(root, "storage-unit", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(RootDirectory);
        _medium = new FileSystemMedium(RootDirectory);
    }

    public string RootDirectory { get; }

    public override IStorageMedium Medium => _medium;

    public override bool IsSimulated => false;

    public override IStorageMedium Restart()
    {
        _medium = new FileSystemMedium(RootDirectory);
        return _medium;
    }

    public override byte[] ReadRaw(string path) => System.IO.File.ReadAllBytes(_medium.GetFullPath(path));

    public override void Corrupt(string path, long offset, byte xorMask = 0xFF)
    {
        using FileStream stream = System.IO.File.Open(_medium.GetFullPath(path), FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        stream.Position = offset;
        int value = stream.ReadByte();
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(offset));
        stream.Position = offset;
        stream.WriteByte((byte)(value ^ xorMask));
    }

    public override ValueTask DisposeAsync()
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                if (System.IO.Directory.Exists(RootDirectory))
                    System.IO.Directory.Delete(RootDirectory, recursive: true);
                break;
            }
            catch (IOException) when (attempt < 4)
            {
                Thread.Sleep(50);
            }
            catch (UnauthorizedAccessException) when (attempt < 4)
            {
                Thread.Sleep(50);
            }
        }

        return default;
    }
}

/// <summary>Base of contract tests that run against both media.</summary>
public abstract class StorageContractTests : IAsyncLifetime
{
    protected MediumHost Host { get; private set; } = null!;

    protected IStorageMedium Medium => Host.Medium;

    protected abstract MediumHost CreateHost();

    public ValueTask InitializeAsync()
    {
        Host = CreateHost();
        return default;
    }

    public async ValueTask DisposeAsync()
    {
        await Host.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}
