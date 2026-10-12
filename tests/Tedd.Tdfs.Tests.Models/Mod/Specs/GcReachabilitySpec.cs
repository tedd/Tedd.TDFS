using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-008: two-phase GC (mark at a committed index, sweep later) against current roots, snapshots, reader pins,
/// staging sessions of unresolved transactions, backup transfer pins and placement transitions (FSD §14,
/// ARCHITECTURE §8, test plan §9.7).
/// </summary>
internal sealed class GcReachabilitySpec : ProtocolSpec<GcReachabilitySpec.State>
{
    private const int Objects = 3;               // o0 committed earlier (staging closed); o1, o2 staged by the transaction
    private static readonly string[] ObjName = ["o0", "o1", "o2"];
    private const int MaxSnapshots = 2;
    private const int MaxPins = 1;
    private const int MaxTransfers = 1;

    private const int NoCopy = 0;
    private const int Unverified = 1;
    private const int Verified = 2;

    /// <summary>GC ignores snapshot roots, reader pins and transfer pins (SafetyMutations.IgnoreSnapshotPins).</summary>
    public const string IgnoreSnapshotsAndPins = "IgnoreSnapshotsAndPins";

    /// <summary>GC treats objects of an open staging session (unresolved transaction) as garbage after a timeout.</summary>
    public const string TimeBasedStagingExpiry = "TimeBasedStagingExpiry";

    /// <summary>A placement transition deletes the old copy before the new copy is verified.</summary>
    public const string DeleteOldCopyBeforeVerify = "DeleteOldCopyBeforeNewVerified";

    internal sealed class State
    {
        public int Root = 0b001;
        public int Snap = -1;        // snapshot root mask, -1 = none
        public int Pin = -1;         // reader pin root mask
        public int Transfer = -1;    // backup transfer pin root mask
        public int TxPhase;          // 0 none, 1 active (staging open), 2 committed, 3 aborted
        public int Uploaded;         // mask of staged objects written
        public bool[] Old = [true, false, false];
        public int[] New = new int[Objects];
        public int Candidates;       // GC mark set
        public int SnapshotsCreated;
        public int PinsTaken;
        public int TransfersStarted;

        public State Clone() => new()
        {
            Root = Root,
            Snap = Snap,
            Pin = Pin,
            Transfer = Transfer,
            TxPhase = TxPhase,
            Uploaded = Uploaded,
            Old = (bool[])Old.Clone(),
            New = (int[])New.Clone(),
            Candidates = Candidates,
            SnapshotsCreated = SnapshotsCreated,
            PinsTaken = PinsTaken,
            TransfersStarted = TransfersStarted,
        };

        public bool HasCopy(int o) => Old[o] || New[o] != NoCopy;

        public bool HasVerifiedCopy(int o) => Old[o] || New[o] == Verified;
    }

    public override string TestId => "MOD-008";
    public override string Name => "GcReachabilityModel";

    public override string Summary =>
        "Object o0 is referenced by the current root; a transaction stages o1/o2 under an open staging session and may " +
        "commit (replacing the root) or abort. Snapshots capture the root, reader pins pin the root, backup transfers pin a " +
        "snapshot root, and placement transitions copy an object to a new node, verify it and delete the old copy. GC marks " +
        "an object deletable at the current committed state (not in the current root, snapshot roots or pinned roots, and its " +
        "staging session closed) and sweeps marked objects later without re-checking.";

    public override SpecBounds Bounds { get; } = new(64, 3_000_000,
        ["objects=3", "transactions=1", $"snapshots<={MaxSnapshots}", $"reader pins<={MaxPins}", $"transfer pins<={MaxTransfers}", "one placement transition per object"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Every object referenced by the current root, a retained snapshot, an active reader pin, an active transfer, or the open staging session of an unresolved transaction keeps at least one verified copy.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "GC decisions are taken against committed (applied) state; the mark and the sweep may be separated by arbitrary later commits and leader changes.",
        "A closed staging session is the authoritative 'no further publication' decision: commits referencing it are rejected.",
        "Object ids are never reused for different content.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "The DerefLog watermark is folded into 'pinned roots are part of R(G)' (equivalent protection for pinned inode versions).",
        "Each object has one original copy and at most one placement-transition copy.",
        "Storage-node inventories are exact (stale inventories are covered by GC/RSY tests).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'TxBegin', 'TxAbort', 'CreateSnapshot', 'DeleteSnapshot', 'Pin', 'Unpin', 'TransferEnd', 'VerifyFail' and 'GcMark' have no dedicated fault site; they are driven through the SDK, TestClock and inventory scheduling (GC-* and CHA-009).",
        "Equivalence of the implementation's DerefLog/watermark mechanism with this reference-set formulation is not proved.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("TxBegin", []),
        ("Upload", [FaultSites.StagingRegistered, FaultSites.ObjectWrite]),
        ("TxCommit", [FaultSites.RaftBeforePropose, FaultSites.RaftAfterCommitBeforeApply]),
        ("TxAbort", []),
        ("CreateSnapshot", []),
        ("DeleteSnapshot", []),
        ("Pin", []),
        ("Unpin", []),
        ("TransferStart", [FaultSites.SnapshotPin, FaultSites.BackupInventory]),
        ("TransferEnd", []),
        ("Move", [FaultSites.Reconfiguration, FaultSites.ObjectWrite]),
        ("Verify", [FaultSites.ObjectAfterFlushBeforeReceipt]),
        ("VerifyFail", []),
        ("RemoveOld", [FaultSites.ReplicaDelete]),
        ("GcMark", []),
        ("GcSweep", [FaultSites.GcSweep]));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(IgnoreSnapshotsAndPins, "GC protects only the current root and open staging", nameof(SafetyMutations.IgnoreSnapshotPins)),
        new(TimeBasedStagingExpiry, "GC deletes unreferenced objects of an open staging session (elapsed time treated as abandonment)"),
        new(DeleteOldCopyBeforeVerify, "placement transition removes the old copy before the new copy is verified"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        if (s.TxPhase == 0)
        {
            State t = s.Clone();
            t.TxPhase = 1;
            yield return ("TxBegin", t);
        }
        else if (s.TxPhase == 1)
        {
            for (int o = 1; o < Objects; o++)
            {
                if ((s.Uploaded & (1 << o)) != 0)
                    continue;
                State t = s.Clone();
                t.Uploaded |= 1 << o;
                t.Old[o] = true;
                yield return ($"Upload({ObjName[o]})", t);
            }

            if (s.Uploaded != 0)
            {
                State t = s.Clone();
                t.Root = s.Uploaded;
                t.TxPhase = 2;
                yield return ($"TxCommit(root={ModHelpers.Set(t.Root, ObjName)})", t);
            }

            State aborted = s.Clone();
            aborted.TxPhase = 3;
            yield return ("TxAbort", aborted);
        }

        if (s.Snap == -1 && s.SnapshotsCreated < MaxSnapshots)
        {
            State t = s.Clone();
            t.Snap = s.Root;
            t.SnapshotsCreated++;
            yield return ($"CreateSnapshot({ModHelpers.Set(t.Snap, ObjName)})", t);
        }

        if (s.Snap != -1)
        {
            State t = s.Clone();
            t.Snap = -1;
            yield return ("DeleteSnapshot", t);
        }

        if (s.Pin == -1 && s.PinsTaken < MaxPins)
        {
            State t = s.Clone();
            t.Pin = s.Root;
            t.PinsTaken++;
            yield return ($"Pin({ModHelpers.Set(t.Pin, ObjName)})", t);
        }

        if (s.Pin != -1)
        {
            State t = s.Clone();
            t.Pin = -1;
            yield return ("Unpin", t);
        }

        if (s.Transfer == -1 && s.Snap != -1 && s.TransfersStarted < MaxTransfers)
        {
            State t = s.Clone();
            t.Transfer = s.Snap;
            t.TransfersStarted++;
            yield return ($"TransferStart({ModHelpers.Set(t.Transfer, ObjName)})", t);
        }

        if (s.Transfer != -1)
        {
            State t = s.Clone();
            t.Transfer = -1;
            yield return ("TransferEnd", t);
        }

        for (int o = 0; o < Objects; o++)
        {
            if (s.Old[o] && s.New[o] == NoCopy)
            {
                State t = s.Clone();
                t.New[o] = Unverified;
                yield return ($"Move({ObjName[o]})", t);
            }

            if (s.New[o] == Unverified)
            {
                State ok = s.Clone();
                ok.New[o] = Verified;
                yield return ($"Verify({ObjName[o]})", ok);

                State failed = s.Clone();
                failed.New[o] = NoCopy;
                yield return ($"VerifyFail({ObjName[o]})", failed);
            }

            bool removeAllowed = variant == DeleteOldCopyBeforeVerify ? s.New[o] != NoCopy : s.New[o] == Verified;
            if (s.Old[o] && removeAllowed)
            {
                State t = s.Clone();
                t.Old[o] = false;
                yield return ($"RemoveOld({ObjName[o]})", t);
            }

            int bit = 1 << o;
            if ((s.Candidates & bit) == 0 && s.HasCopy(o) && Deletable(s, o, variant))
            {
                State t = s.Clone();
                t.Candidates |= bit;
                yield return ($"GcMark({ObjName[o]})", t);
            }

            if ((s.Candidates & bit) != 0)
            {
                State t = s.Clone();
                t.Candidates &= ~bit;
                t.Old[o] = false;
                t.New[o] = NoCopy;
                yield return ($"GcSweep({ObjName[o]})", t);
            }
        }
    }

    private static bool Deletable(State s, int o, string variant)
    {
        int bit = 1 << o;
        int reachable = s.Root;
        if (variant != IgnoreSnapshotsAndPins)
        {
            if (s.Snap != -1)
                reachable |= s.Snap;
            if (s.Pin != -1)
                reachable |= s.Pin;
            if (s.Transfer != -1)
                reachable |= s.Transfer;
        }

        if ((reachable & bit) != 0)
            return false;
        bool stagingClosed = o == 0 || s.TxPhase is 2 or 3;
        return stagingClosed || variant == TimeBasedStagingExpiry;
    }

    protected override string? Invariant(State s, string variant)
    {
        int protectedSet = s.Root;
        if (s.Snap != -1)
            protectedSet |= s.Snap;
        if (s.Pin != -1)
            protectedSet |= s.Pin;
        if (s.Transfer != -1)
            protectedSet |= s.Transfer;
        if (s.TxPhase == 1)
            protectedSet |= s.Uploaded;
        for (int o = 0; o < Objects; o++)
        {
            if ((protectedSet & (1 << o)) != 0 && !s.HasVerifiedCopy(o))
                return $"protected object {ObjName[o]} has no verified copy (root={ModHelpers.Set(s.Root, ObjName)}, snapshot={(s.Snap == -1 ? "-" : ModHelpers.Set(s.Snap, ObjName))}, pin={(s.Pin == -1 ? "-" : ModHelpers.Set(s.Pin, ObjName))}, transfer={(s.Transfer == -1 ? "-" : ModHelpers.Set(s.Transfer, ObjName))}, tx phase {s.TxPhase})";
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(48);
        sb.Append(s.Root).Append('s').Append(s.Snap).Append('p').Append(s.Pin).Append('t').Append(s.Transfer).Append('x').Append(s.TxPhase)
            .Append('u').Append(s.Uploaded).Append('O');
        ModHelpers.AppendBools(sb, s.Old);
        sb.Append('N');
        ModHelpers.AppendInts(sb, s.New);
        sb.Append('c').Append(s.Candidates).Append('#').Append(s.SnapshotsCreated).Append(s.PinsTaken).Append(s.TransfersStarted);
        return sb.ToString();
    }
}
