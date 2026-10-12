using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

internal static class RaftTestData
{
    public const string Dir = "raft/control";
    public static readonly NodeId Voter = new(new Guid("cccccccc-0000-4000-8000-000000000003"));

    public static LogEntryRecord Entry(ulong index, ulong term, int salt = 0)
        => new(index, term, (byte)(index % 3), TestData.Bytes((index * 31) + (ulong)salt + (term * 1_000_003), 5 + (int)(index % 23)));

    public static LogEntryRecord[] Range(ulong from, ulong toInclusive, ulong term, int salt = 0)
    {
        var list = new List<LogEntryRecord>();
        for (ulong i = from; i <= toInclusive; i++)
            list.Add(Entry(i, term, salt));
        return [.. list];
    }

    public static void AssertEntry(LogEntryRecord expected, LogEntryRecord actual)
    {
        Assert.Equal(expected.Index, actual.Index);
        Assert.Equal(expected.Term, actual.Term);
        Assert.Equal(expected.Kind, actual.Kind);
        Assert.Equal(expected.Data.ToArray(), actual.Data.ToArray());
    }

    public static string Fingerprint(RaftLogStore store)
    {
        IReadOnlyList<LogEntryRecord> entries = store.Entries(store.FirstIndex, store.LastIndex + 1, 0);
        return string.Create(CultureInfo.InvariantCulture,
            $"hs={store.HardState.Term}/{store.HardState.Vote}/{store.HardState.Commit};snap={store.Snapshot.Index}/{store.Snapshot.Term}/{Convert.ToHexString(store.Snapshot.ConfState.Span)};")
            + string.Join(",", entries.Select(e => string.Create(CultureInfo.InvariantCulture, $"{e.Index}:{e.Term}:{e.Kind}:{Convert.ToHexString(e.Data.Span)}")));
    }
}

public abstract class RaftLogStoreContractTests : StorageContractTests
{
    private const string Dir = RaftTestData.Dir;

    protected ValueTask<RaftLogStore> OpenAsync(RaftLogStoreOptions? options = null, IStorageMedium? medium = null)
        => RaftLogStore.OpenAsync(medium ?? Medium, Dir, options ?? new RaftLogStoreOptions { SegmentSize = 600 });

    [Fact]
    public async Task OpenEmpty()
    {
        await using RaftLogStore store = await OpenAsync();
        Assert.Equal(1ul, store.FirstIndex);
        Assert.Equal(0ul, store.LastIndex);
        Assert.Equal(0ul, store.LastTerm);
        Assert.Equal(RaftLogHardState.Initial, store.HardState);
        Assert.Equal(RaftLogSnapshotMeta.Empty, store.Snapshot);
        Assert.Equal(0ul, store.TermAt(0));
        Assert.Empty(store.Entries(1, 1, 0));
    }

    [Fact]
    public async Task AppendFlushReopen_AndQueries()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.AppendAsync(RaftTestData.Range(1, 30, 1));
            await store.AppendAsync(RaftTestData.Range(31, 40, 2));
            await store.FlushAsync();
            Assert.Equal(40ul, store.DurableIndex);
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(40ul, reopened.LastIndex);
        Assert.Equal(2ul, reopened.LastTerm);
        Assert.Equal(1ul, reopened.TermAt(30));
        Assert.Equal(2ul, reopened.TermAt(31));
        IReadOnlyList<LogEntryRecord> slice = reopened.Entries(5, 15, 4);
        Assert.Equal(4, slice.Count);
        for (int i = 0; i < 4; i++)
            RaftTestData.AssertEntry(RaftTestData.Entry(5 + (ulong)i, 1), slice[i]);
        Assert.Equal(36, reopened.Entries(5, 41, 0).Count);
        Assert.Throws<ArgumentOutOfRangeException>(() => reopened.Entries(1, 42, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => reopened.TermAt(41));
        Assert.False(reopened.TryGetTerm(41, out _));
    }

    [Fact]
    public async Task ConflictingSuffix_IsReplaced_MatchingPrefixKept()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.AppendAsync(RaftTestData.Range(1, 10, 1));
            await store.FlushAsync();
            // Leader of term 3 sends 6..12; 6..7 match (term 1), 8.. conflicts.
            LogEntryRecord[] batch = [.. RaftTestData.Range(6, 7, 1), .. RaftTestData.Range(8, 12, 3)];
            await store.AppendAsync(batch);
            await store.FlushAsync();
            Assert.Equal(12ul, store.LastIndex);
            Assert.Equal(1ul, store.TermAt(7));
            Assert.Equal(3ul, store.TermAt(8));
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        IReadOnlyList<LogEntryRecord> all = reopened.Entries(1, 13, 0);
        for (ulong i = 1; i <= 7; i++)
            RaftTestData.AssertEntry(RaftTestData.Entry(i, 1), all[(int)i - 1]);
        for (ulong i = 8; i <= 12; i++)
            RaftTestData.AssertEntry(RaftTestData.Entry(i, 3), all[(int)i - 1]);
    }

    [Fact]
    public async Task StaleShorterBatch_DoesNotTruncateMatchingLog()
    {
        await using RaftLogStore store = await OpenAsync();
        await store.AppendAsync(RaftTestData.Range(1, 10, 1));
        await store.AppendAsync(RaftTestData.Range(3, 5, 1));
        Assert.Equal(10ul, store.LastIndex);
    }

    /// <summary>
    /// A batch that conflicts with the log at index 4 but whose term is below the term of entry 3 is invalid (terms
    /// never decrease along a log). It must be rejected before anything is changed: the entries it would have replaced
    /// may already have been acknowledged to a leader and must not be truncated durably by a rejected call.
    /// </summary>
    [Fact]
    public async Task ConflictingBatchWithDecreasingTerm_IsRejectedWithoutTruncating()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.AppendAsync([.. RaftTestData.Range(1, 3, 2), .. RaftTestData.Range(4, 5, 3)]);
            await store.FlushAsync();
            await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync([RaftTestData.Entry(4, 1)]));
            Assert.Equal(5ul, store.LastIndex);
            Assert.Equal(3ul, store.TermAt(5));
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(5ul, reopened.LastIndex);
        foreach (LogEntryRecord e in reopened.Entries(1, 6, 0))
            RaftTestData.AssertEntry(RaftTestData.Entry(e.Index, e.Index <= 3 ? 2ul : 3ul), e);
    }

    [Fact]
    public async Task InvalidBatches_AreRejected()
    {
        await using RaftLogStore store = await OpenAsync();
        await store.AppendAsync(RaftTestData.Range(1, 3, 2));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync([RaftTestData.Entry(5, 2)]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync([RaftTestData.Entry(4, 2), RaftTestData.Entry(6, 2)]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync([RaftTestData.Entry(4, 3), RaftTestData.Entry(5, 2)]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync([RaftTestData.Entry(4, 1)]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync([RaftTestData.Entry(0, 1)]));
        Assert.Equal(3ul, store.LastIndex);
    }

    [Fact]
    public async Task ConflictWithCommittedEntry_IsRefused()
    {
        await using RaftLogStore store = await OpenAsync();
        await store.AppendAsync(RaftTestData.Range(1, 5, 1));
        await store.FlushAsync();
        await store.SaveHardStateAsync(new RaftLogHardState(1, RaftTestData.Voter, 4));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.AppendAsync([RaftTestData.Entry(4, 2)]));
        await store.AppendAsync([RaftTestData.Entry(5, 2)]);
        Assert.Equal(2ul, store.TermAt(5));
    }

    [Fact]
    public async Task HardState_IsDurable_AndCannotOutrunTheDurableLog()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.SaveHardStateAsync(new RaftLogHardState(5, RaftTestData.Voter, 0));
            await store.AppendAsync(RaftTestData.Range(1, 3, 5));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.SaveHardStateAsync(new RaftLogHardState(5, RaftTestData.Voter, 2)));
            await store.FlushAsync();
            await store.SaveHardStateAsync(new RaftLogHardState(6, null, 3));
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(new RaftLogHardState(6, null, 3), reopened.HardState);
    }

    [Fact]
    public async Task CompactThrough_MatchingIndex_KeepsSuffix()
    {
        byte[] conf = [1, 2, 3, 4];
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.AppendAsync(RaftTestData.Range(1, 50, 1));
            await store.FlushAsync();
            await store.CompactThroughAsync(30, 1, conf);
            Assert.Equal(31ul, store.FirstIndex);
            Assert.Equal(50ul, store.LastIndex);
            Assert.Equal(1ul, store.TermAt(30));
            Assert.Throws<ArgumentOutOfRangeException>(() => store.TermAt(29));
            Assert.Throws<ArgumentOutOfRangeException>(() => store.Entries(30, 31, 0));
            await store.CompactThroughAsync(20, 1, conf); // older: no-op
            Assert.Equal(30ul, store.Snapshot.Index);
            await store.AppendAsync(RaftTestData.Range(51, 52, 2));
            await store.FlushAsync();
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(new RaftLogSnapshotMeta(30, 1, conf), reopened.Snapshot);
        Assert.Equal(31ul, reopened.FirstIndex);
        Assert.Equal(52ul, reopened.LastIndex);
        RaftTestData.AssertEntry(RaftTestData.Entry(31, 1), reopened.Entries(31, 32, 0)[0]);
        RaftTestData.AssertEntry(RaftTestData.Entry(52, 2), reopened.Entries(52, 53, 0)[0]);
    }

    [Fact]
    public async Task CompactThrough_ForeignSnapshot_DiscardsWholeLog()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.AppendAsync(RaftTestData.Range(1, 20, 1));
            await store.FlushAsync();
            await store.CompactThroughAsync(100, 4, new byte[] { 9 });
            Assert.Equal(100ul, store.LastIndex);
            Assert.Equal(4ul, store.LastTerm);
            Assert.Equal(101ul, store.FirstIndex);
            await store.AppendAsync(RaftTestData.Range(101, 105, 4));
            await store.FlushAsync();
            await store.SaveHardStateAsync(new RaftLogHardState(4, null, 103));
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(100ul, reopened.Snapshot.Index);
        Assert.Equal(105ul, reopened.LastIndex);
        RaftTestData.AssertEntry(RaftTestData.Entry(101, 4), reopened.Entries(101, 102, 0)[0]);
    }

    [Fact]
    public async Task HardStateCommit_MayReachInstalledSnapshotIndex()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.CompactThroughAsync(80, 5, ReadOnlyMemory<byte>.Empty);
            Assert.Equal(80ul, store.DurableIndex);
            await store.SaveHardStateAsync(new RaftLogHardState(5, RaftTestData.Voter, 80));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.SaveHardStateAsync(new RaftLogHardState(5, RaftTestData.Voter, 81)));
        }

        await using RaftLogStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(80ul, reopened.HardState.Commit);
        Assert.Equal(81ul, reopened.FirstIndex);
        Assert.Equal(5ul, reopened.TermAt(80));
    }

    [Fact]
    public async Task CompactThrough_ConflictingTermAtIndex_DiscardsWholeLog()
    {
        await using RaftLogStore store = await OpenAsync();
        await store.AppendAsync(RaftTestData.Range(1, 20, 1));
        await store.CompactThroughAsync(10, 2, ReadOnlyMemory<byte>.Empty);
        Assert.Equal(10ul, store.LastIndex);
        Assert.Equal(2ul, store.LastTerm);
    }

    [Fact]
    public async Task Encrypted_RoundTrips()
    {
        using var cipher = new RecordCipher(KeyRing.Derive(TestData.Bytes(9, 32), StorageKeys.JournalPurpose), new SeededRandom(4));
        var options = new RaftLogStoreOptions { Cipher = cipher };
        await using (RaftLogStore store = await OpenAsync(options))
        {
            await store.AppendAsync(RaftTestData.Range(1, 10, 1));
            await store.FlushAsync();
        }

        await using RaftLogStore reopened = await OpenAsync(options, Host.Restart());
        RaftTestData.AssertEntry(RaftTestData.Entry(7, 1), reopened.Entries(7, 8, 0)[0]);
        await Assert.ThrowsAsync<DfsRecoveryKeyMissingException>(async () => await OpenAsync(new RaftLogStoreOptions(), Host.Restart()));
    }
}

public sealed class RaftLogStoreSimulatedTests : RaftLogStoreContractTests
{
    private const string Dir = RaftTestData.Dir;

    protected override MediumHost CreateHost() => new SimulatedHost();

    [Fact]
    public async Task CommitBeyondRecoveredLog_IsIntegrityFailure()
    {
        await using (RaftLogStore store = await OpenAsync())
        {
            await store.AppendAsync(RaftTestData.Range(1, 5, 1));
            await store.FlushAsync();
        }

        await AtomicFile.WriteAsync(Medium, Dir + "/" + RaftLogStore.HardStateFileName, new RaftLogHardState(1, null, 9).Encode());
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await OpenAsync());
    }

    [Fact]
    public async Task DecreasingTermsOnDisk_AreIntegrityFailure()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir + "/log", new JournalOptions()))
        {
            byte[] high = new byte[9];
            high[0] = 5;
            byte[] low = new byte[9];
            low[0] = 2;
            await journal.AppendAsync(high);
            await journal.AppendAsync(low);
        }

        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await OpenAsync());
    }

    [Fact]
    public async Task RecoveryFaultSite_IsHit()
    {
        var faults = new RecordingFaults();
        await using RaftLogStore store = await OpenAsync(new RaftLogStoreOptions { Faults = faults, NodeName = "n1", GroupName = "control" });
        FaultContext hit = Assert.Single(faults.Hits);
        Assert.Equal(FaultSites.RecoveryJournalReplay, hit.Site);
        Assert.Equal(FaultStage.F16, hit.Stage);
        Assert.Equal("n1", hit.Node);
        Assert.Equal("control", hit.Volume);
    }

    [Fact]
    public async Task CrashMatrix_AppendFlushAndHardState()
    {
        ulong acknowledged = 0;
        RaftLogHardState acknowledgedState = default;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = 5;
                acknowledgedState = new RaftLogHardState(1, RaftTestData.Voter, 3);
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 400 });
                await store.AppendAsync(RaftTestData.Range(1, 5, 1));
                await store.FlushAsync();
                await store.SaveHardStateAsync(acknowledgedState);
            },
            async medium =>
            {
                RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 400 });
                await store.AppendAsync(RaftTestData.Range(6, 12, 2));
                await store.FlushAsync();
                acknowledged = 12;
                var state = new RaftLogHardState(2, RaftTestData.Voter, 10);
                await store.SaveHardStateAsync(state);
                acknowledgedState = state;
            },
            async (medium, crash) =>
            {
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 400 });
                Assert.InRange(store.LastIndex, acknowledged, 12ul);
                Assert.True(store.HardState == acknowledgedState || store.HardState == new RaftLogHardState(2, RaftTestData.Voter, 10));
                foreach (LogEntryRecord e in store.Entries(1, store.LastIndex + 1, 0))
                    RaftTestData.AssertEntry(RaftTestData.Entry(e.Index, e.Index <= 5 ? 1ul : 2ul), e);
                return RaftTestData.Fingerprint(store);
            });
    }

    [Fact]
    public async Task CrashMatrix_ConflictTruncation()
    {
        bool replaced = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                replaced = false;
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 400 });
                await store.AppendAsync(RaftTestData.Range(1, 15, 1));
                await store.FlushAsync();
                await store.SaveHardStateAsync(new RaftLogHardState(1, null, 6));
            },
            async medium =>
            {
                RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 400 });
                await store.AppendAsync(RaftTestData.Range(8, 11, 3));
                await store.FlushAsync();
                replaced = true;
            },
            async (medium, crash) =>
            {
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 400 });
                Assert.True(store.LastIndex >= 7);
                IReadOnlyList<LogEntryRecord> entries = store.Entries(1, store.LastIndex + 1, 0);
                for (ulong i = 1; i <= 7; i++)
                    RaftTestData.AssertEntry(RaftTestData.Entry(i, 1), entries[(int)i - 1]);
                bool old = store.LastIndex >= 8 && store.TermAt(8) == 1;
                if (old)
                {
                    Assert.False(replaced);
                    foreach (LogEntryRecord e in entries.Skip(7))
                        RaftTestData.AssertEntry(RaftTestData.Entry(e.Index, 1), e);
                }
                else
                {
                    Assert.InRange(store.LastIndex, replaced ? 11ul : 7ul, 11ul);
                    foreach (LogEntryRecord e in entries.Skip(7))
                        RaftTestData.AssertEntry(RaftTestData.Entry(e.Index, 3), e);
                }

                return RaftTestData.Fingerprint(store);
            });
    }

    [Theory]
    [InlineData(25, 1)]
    [InlineData(40, 1)]
    [InlineData(60, 3)]
    public async Task CrashMatrix_CompactOrInstallSnapshot(ulong index, ulong term)
    {
        bool compacted = false;
        byte[] conf = [7, 7];
        await CrashMatrix.RunAsync(
            async medium =>
            {
                compacted = false;
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 300 });
                await store.AppendAsync(RaftTestData.Range(1, 40, 1));
                await store.FlushAsync();
            },
            async medium =>
            {
                RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 300 });
                await store.CompactThroughAsync(index, term, conf);
                compacted = true;
                await store.AppendAsync([RaftTestData.Entry(Math.Max(index, 40) + 1, Math.Max(term, 1))]);
                await store.FlushAsync();
            },
            async (medium, crash) =>
            {
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, new RaftLogStoreOptions { SegmentSize = 300 });
                if (store.Snapshot.Index == 0)
                {
                    Assert.False(compacted);
                    Assert.Equal(40ul, store.LastIndex);
                }
                else
                {
                    Assert.Equal(new RaftLogSnapshotMeta(index, term, conf), store.Snapshot);
                    Assert.Equal(index + 1, store.FirstIndex);
                    Assert.Equal(term, store.TermAt(index));
                }

                foreach (LogEntryRecord e in store.Entries(store.FirstIndex, store.LastIndex + 1, 0))
                {
                    ulong expectedTerm = e.Index <= 40 && store.Snapshot.Index <= 40 ? 1ul : Math.Max(term, 1);
                    if (e.Index == Math.Max(index, 40) + 1)
                        expectedTerm = Math.Max(term, 1);
                    RaftTestData.AssertEntry(RaftTestData.Entry(e.Index, expectedTerm), e);
                }

                return RaftTestData.Fingerprint(store);
            });
    }
}

public sealed class RaftLogStoreFileSystemTests : RaftLogStoreContractTests
{
    protected override MediumHost CreateHost() => new FileSystemHost();
}
