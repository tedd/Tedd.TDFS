using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-005: optimistic transaction validation at the commit index (ARCHITECTURE §5.6): read items are validated by
/// revision, absent-name reads by continued absence, and directory-enumeration predicates by directory revision, against
/// concurrent transactional and ordinary mutations in one volume (FSD §8.1, test plan §9.6).
/// </summary>
internal sealed class TransactionValidationSpec : ProtocolSpec<TransactionValidationSpec.State>
{
    private const int A = 0;
    private const int B = 1;
    private const int C = 2;
    private const int Names = 3;
    private const int Absent = -1;
    private const int MaxOrdinary = 2;
    private static readonly string[] NameOf = ["a", "b", "c"];

    /// <summary>Read items of present names are not validated (lost update / write skew possible).</summary>
    public const string NoReadVersionValidation = "NoReadVersionValidation";

    /// <summary>Absent-name dependencies are not validated (phantom create).</summary>
    public const string SkipAbsentNameValidation = "SkipAbsentNameValidation";

    /// <summary>Directory-enumeration predicates are not validated (SafetyMutations.SkipPredicateValidation).</summary>
    public const string SkipPredicateValidation = "SkipPredicateValidation";

    internal sealed class Tx
    {
        public int Phase;                    // 0 not begun, 1 active, 2 committed, 3 aborted (conflict)
        public int[] SnapVal = new int[Names];
        public int[] SnapRev = new int[Names];
        public int SnapDirRev;

        public Tx Clone() => new() { Phase = Phase, SnapVal = (int[])SnapVal.Clone(), SnapRev = (int[])SnapRev.Clone(), SnapDirRev = SnapDirRev };
    }

    internal sealed class State
    {
        public int[] Val = [1, 1, Absent];
        public int[] Rev = new int[Names];
        public int DirRev;
        public Tx[] Txs = [new(), new()];
        public int Ordinary;
        public string? Violation; // ghost

        public State Clone() => new()
        {
            Val = (int[])Val.Clone(),
            Rev = (int[])Rev.Clone(),
            DirRev = DirRev,
            Txs = Txs.Select(t => t.Clone()).ToArray(),
            Ordinary = Ordinary,
            Violation = Violation,
        };

        public int PresentSet()
        {
            int mask = 0;
            for (int i = 0; i < Names; i++)
            {
                if (Val[i] != Absent)
                    mask |= 1 << i;
            }

            return mask;
        }
    }

    public override string TestId => "MOD-005";
    public override string Name => "TransactionValidationModel";

    public override string Summary =>
        "Directory D holds a=1, b=1 and an absent name c. T1 reads a and b, enumerates D and (write skew) sets a:=0 when both " +
        "are 1. T2 reads a, b and the absent name c, sets b:=0 when both are 1 and creates c when it was absent. Ordinary " +
        "(non-transactional) mutations create/delete c and rewrite a concurrently. Transactions read from the retained root " +
        "at their read index and validate at the commit index; every mutation bumps item and directory revisions.";

    public override SpecBounds Bounds { get; } = new(40, 1_000_000,
        ["names=3 in one directory", "transactions=2 (fixed programs)", $"ordinary mutations<={MaxOrdinary}", "values in {absent,0,1}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Commit-order serializability: every committed transaction's reads, absent-name reads and enumeration predicate equal the state at its commit point (the protocol's serialization point), so the committed history is strictly serializable.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "All ordinary and transactional mutations are applied through the same replicated log and bump the revisions they change (TX-011).",
        "Transaction reads come from the retained root at the transaction's read index (consistent snapshot).",
        "Apply is atomic per log entry; replies follow apply (real-time order follows the log order).",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Reads are performed at begin from the snapshot (equivalent because the snapshot is immutable).",
        "Writer reservations (immediate SharingViolation) are not modelled; they only remove interleavings.",
        "The checked property is the stronger commit-order serializability the protocol claims, not general view serializability.",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'Begin', 'Abort', 'OrdinaryCreateC', 'OrdinaryDeleteC' and 'OrdinaryWriteA' map to request handling without dedicated validation fault sites.",
        "Leader-memory working roots and read-set capture (TxOp) are not modelled; TX and PROP-003 histories check them against SerializabilityChecker.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Begin", []),
        ("Commit", [FaultSites.RaftBeforePropose, FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply]),
        ("Abort", []),
        ("OrdinaryCreateC", []),
        ("OrdinaryDeleteC", []),
        ("OrdinaryWriteA", []));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(NoReadVersionValidation, "read versions of present items are not validated at commit"),
        new(SkipAbsentNameValidation, "absent-name dependencies are not validated at commit"),
        new(SkipPredicateValidation, "directory-enumeration predicates are not validated at commit", nameof(SafetyMutations.SkipPredicateValidation)),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int i = 0; i < 2; i++)
        {
            Tx tx = s.Txs[i];
            if (tx.Phase == 0)
            {
                State t = s.Clone();
                Tx nt = t.Txs[i];
                nt.Phase = 1;
                nt.SnapVal = (int[])s.Val.Clone();
                nt.SnapRev = (int[])s.Rev.Clone();
                nt.SnapDirRev = s.DirRev;
                yield return ($"Begin(T{i + 1})", t);
            }
            else if (tx.Phase == 1)
            {
                State t = s.Clone();
                bool valid = Validate(s, i, variant);
                if (valid)
                {
                    CheckSerializable(t, i);
                    ApplyWrites(t, i);
                    t.Txs[i].Phase = 2;
                    yield return ($"Commit(T{i + 1},ok)", t);
                }
                else
                {
                    t.Txs[i].Phase = 3;
                    yield return ($"Commit(T{i + 1},conflict)", t);
                }

                State aborted = s.Clone();
                aborted.Txs[i].Phase = 3;
                yield return ($"Abort(T{i + 1})", aborted);
            }
        }

        if (s.Ordinary < MaxOrdinary)
        {
            if (s.Val[C] == Absent)
            {
                State t = s.Clone();
                t.Ordinary++;
                t.Val[C] = 0;
                t.Rev[C]++;
                t.DirRev++;
                yield return ("OrdinaryCreateC", t);
            }
            else
            {
                State t = s.Clone();
                t.Ordinary++;
                t.Val[C] = Absent;
                t.Rev[C]++;
                t.DirRev++;
                yield return ("OrdinaryDeleteC", t);
            }

            {
                State t = s.Clone();
                t.Ordinary++;
                t.Val[A] = 1;
                t.Rev[A]++;
                yield return ("OrdinaryWriteA", t);
            }
        }
    }

    private static int[] ReadItems(int tx) => tx == 0 ? [A, B] : [A, B, C];

    private static bool Validate(State s, int i, string variant)
    {
        Tx tx = s.Txs[i];
        foreach (int item in ReadItems(i))
        {
            if (tx.SnapVal[item] == Absent)
            {
                if (variant != SkipAbsentNameValidation && s.Val[item] != Absent)
                    return false;
            }
            else if (variant != NoReadVersionValidation && s.Rev[item] != tx.SnapRev[item])
            {
                return false;
            }
        }

        if (i == 0 && variant != SkipPredicateValidation && s.DirRev != tx.SnapDirRev)
            return false;
        return true;
    }

    private static void CheckSerializable(State t, int i)
    {
        Tx tx = t.Txs[i];
        foreach (int item in ReadItems(i))
        {
            if (tx.SnapVal[item] != t.Val[item])
            {
                t.Violation ??= $"T{i + 1} committed after reading {NameOf[item]}={Show(tx.SnapVal[item])} but {NameOf[item]}={Show(t.Val[item])} at its commit point";
                return;
            }
        }

        if (i == 0)
        {
            int snapSet = 0;
            for (int n = 0; n < Names; n++)
            {
                if (tx.SnapVal[n] != Absent)
                    snapSet |= 1 << n;
            }

            if (snapSet != t.PresentSet())
                t.Violation ??= $"T1 committed after enumerating D={ModHelpers.Set(snapSet, NameOf)} but D={ModHelpers.Set(t.PresentSet(), NameOf)} at its commit point (phantom)";
        }
    }

    private static void ApplyWrites(State t, int i)
    {
        Tx tx = t.Txs[i];
        bool bothOne = tx.SnapVal[A] == 1 && tx.SnapVal[B] == 1;
        if (i == 0)
        {
            if (bothOne)
            {
                t.Val[A] = 0;
                t.Rev[A]++;
            }
        }
        else
        {
            if (bothOne)
            {
                t.Val[B] = 0;
                t.Rev[B]++;
            }

            if (tx.SnapVal[C] == Absent)
            {
                if (t.Val[C] == Absent)
                    t.DirRev++;
                t.Val[C] = 1;
                t.Rev[C]++;
            }
        }
    }

    private static string Show(int v) => v == Absent ? "absent" : v.ToString(System.Globalization.CultureInfo.InvariantCulture);

    protected override string? Invariant(State s, string variant)
    {
        if (s.Violation is not null)
            return "strict serializability: " + s.Violation;
        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(64);
        ModHelpers.AppendInts(sb, s.Val);
        sb.Append('r');
        ModHelpers.AppendInts(sb, s.Rev);
        sb.Append('d').Append(s.DirRev).Append('o').Append(s.Ordinary);
        foreach (Tx tx in s.Txs)
        {
            sb.Append("|").Append(tx.Phase);
            if (tx.Phase == 1)
            {
                sb.Append(':');
                ModHelpers.AppendInts(sb, tx.SnapVal);
                sb.Append(':');
                ModHelpers.AppendInts(sb, tx.SnapRev);
                sb.Append(':').Append(tx.SnapDirRev);
            }
        }

        sb.Append(s.Violation is null ? '.' : '!');
        return sb.ToString();
    }
}
