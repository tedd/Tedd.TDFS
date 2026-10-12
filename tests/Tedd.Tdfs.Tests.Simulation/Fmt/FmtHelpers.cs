using System.Security.Cryptography;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Fmt;

/// <summary>Shared helpers for the FMT simulation tests.</summary>
internal static class FmtHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static TestClusterOptions Deterministic(int nodes) => nodes switch
    {
        3 => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic, ScenarioTimeout = TimeSpan.FromMinutes(10) },
        5 => TestClusterOptions.FiveNodes with { Mode = ClusterMode.Deterministic, ScenarioTimeout = TimeSpan.FromMinutes(10) },
        _ => throw new ArgumentOutOfRangeException(nameof(nodes)),
    };

    /// <summary>Restarts every node, one at a time (quorum is kept), as <paramref name="profile"/>.</summary>
    public static async Task RestartAllAsAsync(TestCluster cluster, SoftwareVersionProfile profile)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            await node.RestartAsSoftwareVersionAsync(profile);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
        }

        Assert.All(cluster.Nodes, n => Assert.Equal(profile.Name, n.SoftwareVersion.Name));
    }

    /// <summary>Byte-exact comparison taking arrays, so call sites may await inside the argument list.</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    public static async Task VerifyFilesAsync(DfsVolume volume, IReadOnlyDictionary<string, byte[]> files, string because)
    {
        foreach ((string path, byte[] content) in files)
            BytesEqual(content, await volume.Files.ReadAllBytesAsync(path, Ct), $"{path}: {because}");
    }

    /// <summary>SHA-256 of every raw durable file on a (stopped) node's disk, for "nothing was written" assertions.</summary>
    public static SortedDictionary<string, string> Fingerprint(TestDisk disk)
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (string file in disk.Files())
            result[file] = Convert.ToHexString(SHA256.HashData(disk.ReadRaw(file)));
        return result;
    }

    /// <summary>True when <paramref name="profile"/> may open an image with the given format record.</summary>
    public static bool CanOpen(SoftwareVersionProfile profile, DiskFormatInspection format) =>
        format.MinimumReaderVersion <= profile.MaxReadableDiskFormatVersion
        && format.HighestComponentVersion <= profile.MaxReadableDiskFormatVersion
        && format.ActivatedIrreversibleFeatures.All(profile.Supports);
}
