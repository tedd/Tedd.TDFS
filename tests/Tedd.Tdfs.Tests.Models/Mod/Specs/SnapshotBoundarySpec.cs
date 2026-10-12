using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-006: snapshot capture racing transaction decisions (FSD §8.3, §11.1–11.2; ARCHITECTURE §5.7): a committed
/// transaction appears wholly before or wholly after every snapshot; every snapshot equals the committed state at its own
/// log position (it neither misses a decision ordered before it nor contains one ordered after it); commit-and-snapshot
/// captures exactly that commit.
/// </summary>
internal sealed class SnapshotBoundarySpec : ProtocolSpec<SnapshotBoundarySpec.State>
{
    private const int Txs = 2;
    private const int MaxSnapshots = 3;
    private const int MaxOrdinary = 2;

    /// <summary>A snapshot copies changed files independently (x at one moment, y later).</summary>
    public const string IndependentFileCopy = "IndependentPerFileCapture";

    /// <summary>A transaction's two writes are published by two separate log entries.</summary>
    public const string TwoEntryTransactionCommit = "TwoEntryTransactionCommit";

    /// <summary>
    /// The snapshot entry is ordered after the latest decision in the log, but captures the root as it was before that
    /// decision was applied (capture races the apply of a commit decision, e.g. taken from a lagging replica root).
    /// </summary>
    public const string CaptureRacesApply = "CaptureRacesApplyOfLatestDecision";

    /// <summary>One snapshot: the captured root and (ghost) the committed root at the snapshot's log position.</summary>
    internal readonly record struct Snap(int X, int Y, int Z, int ExactTx, int BoundaryX, int BoundaryY, int BoundaryZ);

    internal sealed class State
    {
        public int X;
        public int Y;
        public int Z;
        public int PrevX;                        // root before the latest mutating entry (what a racing capture may see)
        public int PrevY;
        public int PrevZ;
        public int[] Phase = new int[Txs];       // 0 idle, 1 active, 2 committed, 3 aborted, 4 half-published (broken variant)
        public List<Snap> Snapshots = [];
        public int PendingX = -1;                // broken variant: snapshot capture in progress (x copied; y and z later)
        public int Ordinary;

        public State Clone() => new()
        {
            X = X,
            Y = Y,
            Z = Z,
            PrevX = PrevX,
            PrevY = PrevY,
            PrevZ = PrevZ,
            Phase = (int[])Phase.Clone(),
            Snapshots = [.. Snapshots],
            PendingX = PendingX,
            Ordinary = Ordinary,
        };

        /// <summary>Applies one mutating log entry (remembering the root before it).</summary>
        public void Apply(int? x, int? y, int? z)
        {
            PrevX = X;
            PrevY = Y;
            PrevZ = Z;
            X = x ?? X;
            Y = y ?? Y;
            Z = z ?? Z;
        }
    }

    public override string TestId => "MOD-006";
    public override string Name => "SnapshotConsistentBoundaryModel";

    public override string Summary =>
        "Two transactions each write files /x and /y to their own id; an ordinary mutation toggles /z. Transactions begin, " +
        "commit (one log entry), commit-and-snapshot (one entry that also captures the snapshot), or abort, in any " +
        "interleaving with ordinary snapshots (one entry capturing the current root) and ordinary mutations. A ghost records, " +
        "for every snapshot, the committed root at its log position.";

    public override SpecBounds Bounds { get; } = new(40, 1_000_000,
        ["transactions=2 (each writes x and y)", $"snapshots<={MaxSnapshots}", $"ordinary mutations of z<={MaxOrdinary}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "No snapshot cuts through a committed transaction: in every snapshot x and y come from the same transaction (or both initial).",
        "Every snapshot equals the committed root at its log position: it contains every decision ordered before it and none ordered after it.",
        "A commit-and-snapshot captures exactly its own transaction's writes.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Snapshots and transaction commits are totally ordered by the volume log; each entry is applied atomically.",
        "Uncommitted transactions and unflushed client buffers are invisible to snapshots (they are not in the root).",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "File contents are reduced to the id of the transaction that last wrote them (z: the ordinary-mutation toggle).",
        "Snapshot retention and GC are covered by MOD-008.",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'Begin', 'Abort' and 'WriteZ' map to ordinary request handling without a snapshot-specific fault site.",
        "That CreateSnapshot retains the applied root of exactly its entry (structural sharing) is not proved; SNP/PROP-005 histories check snapshot contents against reference boundaries.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Begin", []),
        ("Commit", [FaultSites.RaftBeforePropose, FaultSites.RaftAfterCommitBeforeApply]),
        ("CommitWithSnapshot", [FaultSites.RaftBeforePropose, FaultSites.RaftAfterCommitBeforeApply, FaultSites.SnapshotPin]),
        ("Abort", []),
        ("WriteZ", []),
        ("Snapshot", [FaultSites.SnapshotPin, FaultSites.RaftAfterCommitBeforeApply]));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(IndependentFileCopy, "snapshot copies changed files independently instead of capturing one committed root"),
        new(TwoEntryTransactionCommit, "a transaction is published as one log entry per file"),
        new(CaptureRacesApply, "the snapshot entry is ordered after the latest decision but captures the root from before its apply"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int i = 0; i < Txs; i++)
        {
            int id = i + 1;
            switch (s.Phase[i])
            {
                case 0:
                {
                    State t = s.Clone();
                    t.Phase[i] = 1;
                    yield return ($"Begin(T{id})", t);
                    break;
                }

                case 1:
                {
                    if (variant == TwoEntryTransactionCommit)
                    {
                        State half = s.Clone();
                        half.Apply(id, null, null);
                        half.Phase[i] = 4;
                        yield return ($"Commit(T{id},x-entry)", half);
                    }
                    else
                    {
                        State t = s.Clone();
                        t.Apply(id, id, null);
                        t.Phase[i] = 2;
                        yield return ($"Commit(T{id})", t);
                    }

                    if (s.Snapshots.Count < MaxSnapshots)
                    {
                        State t = s.Clone();
                        t.Apply(id, id, null);
                        t.Phase[i] = 2;
                        t.Snapshots.Add(new Snap(t.X, t.Y, t.Z, id, t.X, t.Y, t.Z));
                        yield return ($"CommitWithSnapshot(T{id},S{t.Snapshots.Count})", t);
                    }

                    State aborted = s.Clone();
                    aborted.Phase[i] = 3;
                    yield return ($"Abort(T{id})", aborted);
                    break;
                }

                case 4:
                {
                    State t = s.Clone();
                    t.Apply(null, id, null);
                    t.Phase[i] = 2;
                    yield return ($"Commit(T{id},y-entry)", t);
                    break;
                }
            }
        }

        if (s.Ordinary < MaxOrdinary)
        {
            State t = s.Clone();
            t.Ordinary++;
            t.Apply(null, null, 1 - t.Z);
            yield return ("WriteZ", t);
        }

        if (variant == IndependentFileCopy)
        {
            if (s.PendingX == -1 && s.Snapshots.Count < MaxSnapshots)
            {
                State t = s.Clone();
                t.PendingX = s.X;
                yield return ("Snapshot(copy-x)", t);
            }
            else if (s.PendingX != -1)
            {
                State t = s.Clone();
                // The snapshot's log position is where its capture completed.
                t.Snapshots.Add(new Snap(s.PendingX, s.Y, s.Z, 0, s.X, s.Y, s.Z));
                t.PendingX = -1;
                yield return ($"Snapshot(copy-y,S{t.Snapshots.Count})", t);
            }
        }
        else if (s.Snapshots.Count < MaxSnapshots)
        {
            State t = s.Clone();
            t.Snapshots.Add(variant == CaptureRacesApply
                ? new Snap(s.PrevX, s.PrevY, s.PrevZ, 0, s.X, s.Y, s.Z)
                : new Snap(s.X, s.Y, s.Z, 0, s.X, s.Y, s.Z));
            yield return ($"Snapshot(S{t.Snapshots.Count})", t);
        }
    }

    protected override string? Invariant(State s, string variant)
    {
        for (int k = 0; k < s.Snapshots.Count; k++)
        {
            Snap snap = s.Snapshots[k];
            if (snap.X != snap.Y)
                return $"snapshot S{k + 1} cuts through a transaction: x from T{snap.X}, y from T{snap.Y}";
            if (snap.ExactTx != 0 && (snap.X != snap.ExactTx || snap.Y != snap.ExactTx))
                return $"commit-and-snapshot S{k + 1} of T{snap.ExactTx} captured x=T{snap.X}, y=T{snap.Y}";
            if (snap.X != snap.BoundaryX || snap.Y != snap.BoundaryY || snap.Z != snap.BoundaryZ)
            {
                return $"snapshot S{k + 1} does not match its log position: captured x=T{snap.X}, y=T{snap.Y}, z={snap.Z} " +
                    $"but the committed root at its position is x=T{snap.BoundaryX}, y=T{snap.BoundaryY}, z={snap.BoundaryZ}";
            }
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(64);
        sb.Append(s.X).Append(s.Y).Append(s.Z).Append('<').Append(s.PrevX).Append(s.PrevY).Append(s.PrevZ).Append('P');
        ModHelpers.AppendInts(sb, s.Phase);
        sb.Append('S');
        foreach (Snap snap in s.Snapshots)
        {
            sb.Append(snap.X).Append(snap.Y).Append(snap.Z).Append(snap.ExactTx).Append('@')
                .Append(snap.BoundaryX).Append(snap.BoundaryY).Append(snap.BoundaryZ).Append(';');
        }

        sb.Append('p').Append(s.PendingX).Append('o').Append(s.Ordinary);
        return sb.ToString();
    }
}
