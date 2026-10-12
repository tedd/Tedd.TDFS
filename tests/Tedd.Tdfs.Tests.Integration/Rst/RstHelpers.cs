using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Rst;

/// <summary>Shared setup for RST integration scenarios (real QUIC transport, real disk storage).</summary>
internal static class RstHelpers
{
    public const string Vol = RecoveryScenario.Volume;

    /// <summary>Source cluster: <paramref name="nodeCount"/> nodes one per zone, QUIC + disk, backup nodes, one volume with the fixture's named policy.</summary>
    public static TestClusterOptions SourceOptions(int nodeCount = 3, int backups = 1, int inlineThreshold = 4096) => new()
    {
        NodeCount = nodeCount,
        ControlVoters = Math.Min(nodeCount, 3),
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        BackupNodeCount = backups,
        Volumes = [new TestVolumeSpec(Vol) { InlineThreshold = inlineThreshold, NamedPolicies = VolumeContentFixture.NamedPolicies }],
    };

    /// <summary>Recovery ("clean host") cluster: nodes r0.. one per zone, QUIC + disk, no volumes.</summary>
    public static TestClusterOptions RecoveryOptions(int nodeCount = 3, int backups = 0, bool outOfProcess = false) => new()
    {
        Nodes = Enumerable.Range(0, nodeCount).Select(i => new TestNodeSpec("r" + i.ToString(CultureInfo.InvariantCulture), "z" + (i + 1).ToString(CultureInfo.InvariantCulture))).ToArray(),
        ControlVoters = Math.Min(nodeCount, 3),
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        Volumes = [],
        BackupNodeCount = backups,
        OutOfProcess = outOfProcess,
    };

    /// <summary>Connects a read-only client to a backup host as its reader principal.</summary>
    public static Task<DfsClient> ConnectBackupReaderAsync(TestCluster cluster, TestBackupNode backup, string name, CancellationToken ct) =>
        cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = name, Credentials = backup.ReaderCredentials }, ct);

    /// <summary>Captures a volume through a fresh client (no caches shared with any earlier client).</summary>
    public static async Task<ObservedVolumeContent> CaptureFreshAsync(TestCluster cluster, string volume, string clientName, CancellationToken ct)
    {
        await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = clientName }, ct);
        return await VolumeContentFixture.CaptureAsync(client.GetVolume(volume), ct);
    }

    /// <summary>Asserts no extent of the file overlaps any of its holes (sparse ranges are not materialised).</summary>
    public static async Task AssertHolesUnmappedAsync(TestCluster cluster, string volume, ExpectedFile file)
    {
        FileInspection? inspection = await cluster.Inspect.GetFileAsync(volume, file.Path);
        Assert.NotNull(inspection);
        Assert.Equal(file.Content.LongLength, inspection.Length);
        foreach ((long offset, long length) in file.Holes)
        {
            foreach (ExtentInspection extent in inspection.Extents)
            {
                bool overlaps = extent.Offset < offset + length && offset < extent.Offset + extent.Length;
                Assert.False(overlaps,
                    $"{file.Path}: extent [{extent.Offset},{extent.Offset + extent.Length}) maps hole [{offset},{offset + length}) — restore materialised a sparse range");
            }
        }
    }

    // ---- RST-003 transaction ledger -------------------------------------------------------------------------------

    public const int Accounts = 4;
    public const string Counter = "/tx/counter";

    public static string Account(int i) => "/tx/acct-" + i.ToString(CultureInfo.InvariantCulture);

    public static string TokenFile(int generation) => "/tx/token-" + generation.ToString(CultureInfo.InvariantCulture);

    public static string GenerationDirectory(int generation) => "/tx/gen-" + generation.ToString(CultureInfo.InvariantCulture);

    /// <summary>Account content for a generation; larger than the default inline threshold so transactions publish extents too.</summary>
    public static string GenerationText(int generation) =>
        "gen=" + generation.ToString(CultureInfo.InvariantCulture) + ";" + new string((char)('a' + (generation % 26)), 5000);

    /// <summary>Creates generation 0 of the ledger.</summary>
    public static async Task InitLedgerAsync(DfsVolume volume, CancellationToken ct)
    {
        await volume.Directories.CreateDirectoryAsync("/tx", ct);
        for (int i = 0; i < Accounts; i++)
            await volume.Files.WriteAllTextAsync(Account(i), GenerationText(0), null, ct);
        await volume.Files.WriteAllTextAsync(Counter, "0", null, ct);
        await volume.Files.WriteAllTextAsync(TokenFile(0), "token", null, ct);
    }

    /// <summary>
    /// Stages generation <paramref name="generation"/> in <paramref name="tx"/>: reads every account (asserting one
    /// consistent previous generation), rewrites all accounts and the counter, renames the token file and creates the
    /// generation directory.
    /// </summary>
    public static async Task StageGenerationAsync(DfsTransaction tx, int generation, CancellationToken ct)
    {
        int previous = int.Parse(await tx.Files.ReadAllTextAsync(Counter, null, ct), CultureInfo.InvariantCulture);
        for (int i = 0; i < Accounts; i++)
        {
            string current = await tx.Files.ReadAllTextAsync(Account(i), null, ct);
            Assert.Equal(GenerationText(previous), current);
            await tx.Files.WriteAllTextAsync(Account(i), GenerationText(generation), null, ct);
        }

        await tx.Files.WriteAllTextAsync(Counter, generation.ToString(CultureInfo.InvariantCulture), null, ct);
        await tx.Files.MoveAsync(TokenFile(previous), TokenFile(generation), false, ct);
        await tx.Directories.CreateDirectoryAsync(GenerationDirectory(generation), ct);
    }

    /// <summary>Begins, stages and commits one generation; returns its commit token.</summary>
    public static async Task<CommitToken> CommitGenerationAsync(DfsVolume volume, int generation, CancellationToken ct)
    {
        await using DfsTransaction tx = await volume.Transactions.BeginAsync(null, ct);
        await StageGenerationAsync(tx, generation, ct);
        CommitToken token = await tx.CommitAsync(ct);
        Assert.Equal(DfsTransactionState.Committed, tx.State);
        return token;
    }

    /// <summary>
    /// Asserts the ledger shows exactly generation <paramref name="generation"/> and every transaction up to it in whole:
    /// all accounts and the counter at that generation, exactly one token file, generation directories 1..g and nothing
    /// from later, aborted or uncommitted transactions.
    /// </summary>
    public static async Task AssertLedgerAsync(DfsFileClient files, DfsDirectoryClient directories, int generation, int maxGenerationAttempted, string because, CancellationToken ct)
    {
        for (int i = 0; i < Accounts; i++)
            Assert.True(string.Equals(GenerationText(generation), await files.ReadAllTextAsync(Account(i), null, ct), StringComparison.Ordinal),
                $"{Account(i)} is not at generation {generation} ({because})");
        Assert.Equal(generation.ToString(CultureInfo.InvariantCulture), await files.ReadAllTextAsync(Counter, null, ct));
        var names = new List<string>();
        await foreach (DfsEntryInfo e in directories.EnumerateAsync("/tx", null, ct))
            names.Add(e.Name);
        Assert.Single(names, n => n.StartsWith("token-", StringComparison.Ordinal));
        Assert.Contains("token-" + generation.ToString(CultureInfo.InvariantCulture), names);
        for (int g = 1; g <= maxGenerationAttempted; g++)
        {
            bool expected = g <= generation;
            Assert.True(expected == names.Contains("gen-" + g.ToString(CultureInfo.InvariantCulture)),
                $"generation directory gen-{g} presence must be {expected} for boundary generation {generation} ({because})");
        }

        Assert.DoesNotContain(names, n => n.StartsWith("pending", StringComparison.Ordinal));
        Assert.DoesNotContain(names, n => n is "gen-900" or "gen-999" or "token-900" or "token-999");
    }
}
