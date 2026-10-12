using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-006: snapshot capture racing transaction decisions (FSD §8.3, §11.1–11.2; ARCHITECTURE §5.7): a committed
/// transaction appears wholly before or wholly after every snapshot; commit-and-snapshot captures exactly that commit.
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

    internal sealed class State
    {
        public int X;
        public int Y;
        public int Z;
        public int[] Phase = new int[Txs];       // 0 idle, 1 active, 2 committed, 3 aborted, 4 half-published (broken variant)
        public List<(int X, int Y, int ExactTx)> Snapshots = [];
        public int PendingX = -1;                // broken variant: snapshot capture in progress
        public int Ordinary;

        public State Clone() => new()
        {
            X = X,
            Y = Y,
            Z = Z,
            Phase = (int[])Phase.Clone(),
            Snapshots = [.. Snapshots],
            PendingX = PendingX,
            Ordinary = Ordinary,
        };
    }

    public override string TestId => "MOD-006";
    public override string Name => "SnapshotConsistentBoundaryModel";

    public override string Summary =>
        "Two transactions each write files /x and /y to their own id; an ordinary mutation writes /z. Transactions begin, " +
        "commit (one log entry), commit-and-snapshot (one entry that also captures the snapshot), or abort, in any " +
        "interleaving with ordinary snapshots (one entry capturing the current root) and ordinary mutations.";

    public override SpecBounds Bounds { get; } = new(40, 1_000_000,
        ["transactions=2 (each writes x and y)", $"snapshots<={MaxSnapshots}", $"ordinary mutations of z<={MaxOrdinary}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "No snapshot cuts through a committed transaction: in every snapshot x and y come from the same transaction (or both initial).",
        "A commit-and-snapshot captures exactly its own transaction's writes.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Snapshots and transaction commits are totally ordered by the volume log; each entry is applied atomically.",
        "Uncommitted transactions and unflushed client buffers are invisible to snapshots (they are not in the root).",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "File contents are reduced to the id of the transaction that last wrote them.",
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
                        half.X = id;
                        half.Phase[i] = 4;
                        yield return ($"Commit(T{id},x-entry)", half);
                    }
                    else
                    {
                        State t = s.Clone();
                        t.X = id;
                        t.Y = id;
                        t.Phase[i] = 2;
                        yield return ($"Commit(T{id})", t);
                    }

                    if (s.Snapshots.Count < MaxSnapshots)
                    {
                        State t = s.Clone();
                        t.X = id;
                        t.Y = id;
                        t.Phase[i] = 2;
                        t.Snapshots.Add((t.X, t.Y, id));
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
                    t.Y = id;
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
            t.Z = 1 - t.Z;
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
                t.Snapshots.Add((s.PendingX, s.Y, 0));
                t.PendingX = -1;
                yield return ($"Snapshot(copy-y,S{t.Snapshots.Count})", t);
            }
        }
        else if (s.Snapshots.Count < MaxSnapshots)
        {
            State t = s.Clone();
            t.Snapshots.Add((s.X, s.Y, 0));
            yield return ($"Snapshot(S{t.Snapshots.Count})", t);
        }
    }

    protected override string? Invariant(State s, string variant)
    {
        for (int k = 0; k < s.Snapshots.Count; k++)
        {
            (int x, int y, int exact) = s.Snapshots[k];
            if (x != y)
                return $"snapshot S{k + 1} cuts through a transaction: x from T{x}, y from T{y}";
            if (exact != 0 && (x != exact || y != exact))
                return $"commit-and-snapshot S{k + 1} of T{exact} captured x=T{x}, y=T{y}";
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(48);
        sb.Append(s.X).Append(s.Y).Append(s.Z).Append('P');
        ModHelpers.AppendInts(sb, s.Phase);
        sb.Append('S');
        foreach ((int x, int y, int e) in s.Snapshots)
            sb.Append(x).Append(y).Append(e).Append(';');
        sb.Append('p').Append(s.PendingX).Append('o').Append(s.Ordinary);
        return sb.ToString();
    }
}
