using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-003: data-then-metadata publication (FSD §10.3, ARCHITECTURE §5.4/§6/§8): staged object uploads, local flush,
/// receipts, commit decision, storage-node crashes losing unflushed data, staging-session close and GC.
/// </summary>
internal sealed class DataThenMetadataSpec : ProtocolSpec<DataThenMetadataSpec.State>
{
    private const int Objects = 2;
    private const int Nodes = 3;          // one node per zone
    private const int AckCopies = 2;      // durableCopiesBeforeAck = durableZonesBeforeAck = 2
    private const int MaxCommits = 2;
    private const int MaxCrashes = 2;

    private const int None = 0;
    private const int Volatile = 1;
    private const int Durable = 2;

    /// <summary>Receipts are issued before the durability primitive completes (SafetyMutations.AcknowledgeBeforeFlush).</summary>
    public const string AcknowledgeBeforeFlush = "AcknowledgeBeforeFlush";

    /// <summary>The commit is proposed without validated receipts (SafetyMutations.CommitMetadataBeforeData).</summary>
    public const string CommitMetadataBeforeData = "CommitMetadataBeforeData";

    /// <summary>GC treats unreferenced objects of an open staging session as garbage (time alone authorises deletion).</summary>
    public const string GcIgnoresOpenStaging = "GcIgnoresOpenStaging";

    internal sealed class State
    {
        public int[] Copy = new int[Objects * Nodes];
        public bool[] Receipt = new bool[Objects * Nodes];
        public bool StagingClosed;
        public int Root;          // bit mask of objects referenced by the published root
        public int Commits;
        public int Crashes;

        public State Clone() => new()
        {
            Copy = (int[])Copy.Clone(),
            Receipt = (bool[])Receipt.Clone(),
            StagingClosed = StagingClosed,
            Root = Root,
            Commits = Commits,
            Crashes = Crashes,
        };
    }

    public override string TestId => "MOD-003";
    public override string Name => "DataThenMetadataModel";

    public override string Summary =>
        "Two staged objects, three storage nodes in three zones, policy ack = 2 copies in 2 zones. Objects are written " +
        "volatile, flushed, and receipted; the metadata leader validates receipts before proposing the commit that publishes " +
        "a root; storage nodes crash losing unflushed writes; the staging session closes; GC deletes copies of objects that are " +
        "unreferenced and whose staging session is closed.";

    public override SpecBounds Bounds { get; } = new(64, 3_000_000,
        ["objects=2", "storage nodes=3 (3 zones)", "ack policy=2 copies/2 zones", $"commits<={MaxCommits}", $"node crashes<={MaxCrashes}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Every object referenced by the published root has at least 2 durable copies in 2 distinct zones (no published root references insufficiently protected data).",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "A completed durability primitive (flush) survives crashes (supported storage-stack contract, test plan §3.1).",
        "Permanent destruction of durable copies is out of the declared envelope (test plan §3.2).",
        "Receipts are authenticated (MAC) and bound to object id/checksum/node incarnation, so they cannot be forged or replayed for other content.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Receipt validation and the commit proposal are one atomic step at the leader.",
        "Each node is its own zone; host/zone deduplication is covered by REP tests.",
        "Repair and re-replication are not modelled (they only add copies).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'Crash' and 'CloseStaging' have no dedicated fault site: crashes are injected with FaultAction.CrashNode at object.write/object.before-flush, and staging close is driven by handle close/expiry (cleanup.release covers the release path).",
        "That ObjectStore issues receipts only after its segment flush is not proved here (MUT AcknowledgeBeforeFlush, DSK tests).",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Upload", [FaultSites.ClientUploadChunk, FaultSites.ObjectWrite]),
        ("Flush", [FaultSites.ObjectBeforeFlush]),
        ("Receipt", [FaultSites.ObjectAfterFlushBeforeReceipt, FaultSites.CommitReceiptValidated]),
        ("Commit", [FaultSites.CommitReceiptValidated, FaultSites.RaftBeforePropose, FaultSites.RaftAfterLocalAppend, FaultSites.RaftAfterCommitBeforeApply]),
        ("Crash", []),
        ("CloseStaging", []),
        ("Gc", [FaultSites.GcSweep, FaultSites.ReplicaDelete]));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(AcknowledgeBeforeFlush, "storage receipts issued for volatile (unflushed) copies", nameof(SafetyMutations.AcknowledgeBeforeFlush)),
        new(CommitMetadataBeforeData, "metadata commit proposed without sufficient validated receipts", nameof(SafetyMutations.CommitMetadataBeforeData)),
        new(GcIgnoresOpenStaging, "GC deletes unreferenced objects while their staging session is still open"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int o = 0; o < Objects; o++)
        {
            for (int n = 0; n < Nodes; n++)
            {
                int i = o * Nodes + n;
                if (!s.StagingClosed && s.Copy[i] == None)
                {
                    State t = s.Clone();
                    t.Copy[i] = Volatile;
                    yield return ($"Upload(o{o},n{n})", t);
                }

                if (s.Copy[i] == Volatile)
                {
                    State t = s.Clone();
                    t.Copy[i] = Durable;
                    yield return ($"Flush(o{o},n{n})", t);
                }

                bool receiptAllowed = variant == AcknowledgeBeforeFlush ? s.Copy[i] != None : s.Copy[i] == Durable;
                if (receiptAllowed && !s.Receipt[i])
                {
                    State t = s.Clone();
                    t.Receipt[i] = true;
                    yield return ($"Receipt(o{o},n{n})", t);
                }

                bool gcAllowed = (s.Root & (1 << o)) == 0 && (s.StagingClosed || variant == GcIgnoresOpenStaging);
                if (gcAllowed && s.Copy[i] != None)
                {
                    State t = s.Clone();
                    t.Copy[i] = None; // receipts already handed out are not revoked by deletion
                    yield return ($"Gc(o{o},n{n})", t);
                }
            }
        }

        if (!s.StagingClosed && s.Commits < MaxCommits)
        {
            for (int set = 1; set < 1 << Objects; set++)
            {
                if (set == s.Root)
                    continue;
                bool ok = true;
                for (int o = 0; o < Objects && ok; o++)
                {
                    if ((set & (1 << o)) == 0)
                        continue;
                    int receipts = 0;
                    for (int n = 0; n < Nodes; n++)
                    {
                        if (s.Receipt[o * Nodes + n])
                            receipts++;
                    }

                    ok = variant == CommitMetadataBeforeData || receipts >= AckCopies;
                }

                if (!ok)
                    continue;
                State t = s.Clone();
                t.Root = set;
                t.Commits++;
                yield return ($"Commit({ModHelpers.Set(set, ["o0", "o1"])})", t);
            }
        }

        if (!s.StagingClosed)
        {
            State t = s.Clone();
            t.StagingClosed = true;
            yield return ("CloseStaging", t);
        }

        if (s.Crashes < MaxCrashes)
        {
            for (int n = 0; n < Nodes; n++)
            {
                bool any = false;
                for (int o = 0; o < Objects; o++)
                    any |= s.Copy[o * Nodes + n] == Volatile;
                if (!any)
                    continue;
                State t = s.Clone();
                t.Crashes++;
                for (int o = 0; o < Objects; o++)
                {
                    if (t.Copy[o * Nodes + n] == Volatile)
                        t.Copy[o * Nodes + n] = None;
                }

                yield return ($"Crash(n{n})", t);
            }
        }
    }

    protected override string? Invariant(State s, string variant)
    {
        for (int o = 0; o < Objects; o++)
        {
            if ((s.Root & (1 << o)) == 0)
                continue;
            int durable = 0;
            for (int n = 0; n < Nodes; n++)
            {
                if (s.Copy[o * Nodes + n] == Durable)
                    durable++;
            }

            if (durable < AckCopies)
                return $"published root references o{o} with only {durable} durable copies (policy requires {AckCopies} in {AckCopies} zones)";
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(32);
        ModHelpers.AppendInts(sb, s.Copy);
        sb.Append('R');
        ModHelpers.AppendBools(sb, s.Receipt);
        sb.Append(s.StagingClosed ? 'C' : 'O').Append(s.Root).Append('c').Append(s.Commits).Append('x').Append(s.Crashes);
        return sb.ToString();
    }
}
