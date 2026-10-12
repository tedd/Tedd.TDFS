using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-007: late-sync backup transfer and publication (FSD §12.2–12.4, ARCHITECTURE §12, test plan §9.5): partial
/// transfer, destination crash, false inventory hints, closure verification, atomic published-pointer update,
/// acknowledgement, retention expiry and destination GC.
/// </summary>
internal sealed class BackupPublishSpec : ProtocolSpec<BackupPublishSpec.State>
{
    private const int Objects = 3;
    private const int Snapshots = 2;
    private static readonly int[] Deps = [0b011, 0b110]; // S1 = {o1,o2}, S2 = {o2,o3}
    private static readonly string[] SnapName = ["S1", "S2"];
    private const int MaxCrashes = 2;
    private const int MaxHints = 1;

    private const int None = 0;
    private const int Received = 1;   // staged, not yet durable
    private const int Durable = 2;

    /// <summary>The destination trusts the exporter's inventory hints instead of verifying closure (SafetyMutations.PublishBackupBeforeClosure).</summary>
    public const string PublishTrustingInventoryHints = "PublishTrustingInventoryHints";

    /// <summary>The destination publishes once objects are received, before they are durable.</summary>
    public const string PublishBeforeDurable = "PublishBeforeDurable";

    /// <summary>Destination GC keeps only objects of the latest published snapshot (older retained snapshots ignored).</summary>
    public const string GcConsidersLatestSnapshotOnly = "GcConsidersLatestSnapshotOnly";

    internal sealed class State
    {
        public int[] Obj = new int[Objects];
        public int[] Manifest = new int[Snapshots];
        public int Published;    // mask
        public int Acked;        // mask
        public int Pointer;      // 0 none, else snapshot number
        public int Expired;      // mask: retention period elapsed
        public int Pruned;       // mask
        public int Transferring; // 0 none, else snapshot number
        public int Hints;        // mask of objects the exporter wrongly believes the destination holds
        public int HintsUsed;
        public int Crashes;

        public State Clone() => new()
        {
            Obj = (int[])Obj.Clone(),
            Manifest = (int[])Manifest.Clone(),
            Published = Published,
            Acked = Acked,
            Pointer = Pointer,
            Expired = Expired,
            Pruned = Pruned,
            Transferring = Transferring,
            Hints = Hints,
            HintsUsed = HintsUsed,
            Crashes = Crashes,
        };
    }

    public override string TestId => "MOD-007";
    public override string Name => "BackupPublishModel";

    public override string Summary =>
        "Two source snapshots with shared dependencies (S1={o1,o2}, S2={o2,o3}) are exported to one destination. The exporter " +
        "sends missing objects (skipping objects its inventory hint claims are present — hints may be false), then the " +
        "manifest. The destination stages, flushes, verifies the complete dependency closure itself, atomically advances its " +
        "published pointer and only then acknowledges. The destination may crash (losing staged data), retention periods may " +
        "elapse, expired non-latest snapshots may be pruned and destination GC reclaims unreferenced objects.";

    public override SpecBounds Bounds { get; } = new(64, 3_000_000,
        ["source snapshots=2 (shared dependency)", "objects=3", $"destination crashes<={MaxCrashes}", $"false inventory hints<={MaxHints}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Every published snapshot has a durable manifest and its entire durable dependency closure.",
        "Every acknowledged snapshot is published, or was pruned only after its retention expired (retained as promised).",
        "The published pointer always names a published snapshot.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Pointer replacement is atomic (temp + flush + rename + directory flush, ARCHITECTURE §12).",
        "A completed flush survives destination crashes; destination disk destruction is out of envelope.",
        "Retention expiry is decided by the destination's own retention policy, never by source credentials.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Encryption, checksums and per-object verification are folded into 'Durable' (verified) object state.",
        "Bandwidth/pin budgets and reseed lineages are covered by PROP-006 and BAK tests.",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'StartTransfer', 'FalseInventoryHint', 'RetentionElapses', 'Prune' and 'Crash' have no dedicated fault site (scheduler, inventory content, TestClock and FaultAction.CrashNode on the backup node).",
        "That BackupNodeHost verifies closure from its own durable store (not from hints) is not proved here (MUT PublishBackupBeforeClosure, BAK-007).",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("StartTransfer", []),
        ("FalseInventoryHint", []),
        ("SendObject", [FaultSites.BackupInventory, FaultSites.BackupObjectReceived]),
        ("FlushObject", [FaultSites.BackupObjectReceived, FaultSites.BackupBeforeClosureVerify]),
        ("SendManifest", [FaultSites.BackupObjectReceived]),
        ("FlushManifest", [FaultSites.BackupBeforeClosureVerify]),
        ("Publish", [FaultSites.BackupBeforeClosureVerify, FaultSites.BackupBeforePointer, FaultSites.BackupAfterPointer, FaultSites.BackupAck]),
        ("Crash", []),
        ("RetentionElapses", []),
        ("Prune", []),
        ("BackupGc", [FaultSites.GcSweep]));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(PublishTrustingInventoryHints, "publication trusts inventory hints instead of verifying the closure", nameof(SafetyMutations.PublishBackupBeforeClosure)),
        new(PublishBeforeDurable, "publication before received objects/manifest are durable"),
        new(GcConsidersLatestSnapshotOnly, "destination GC protects only the latest published snapshot"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        if (s.Transferring == 0)
        {
            for (int k = 0; k < Snapshots; k++)
            {
                int bit = 1 << k;
                if ((s.Published & bit) != 0 || (s.Pruned & bit) != 0)
                    continue;
                State t = s.Clone();
                t.Transferring = k + 1;
                yield return ($"StartTransfer({SnapName[k]})", t);
            }
        }
        else
        {
            int k = s.Transferring - 1;
            for (int o = 0; o < Objects; o++)
            {
                if ((Deps[k] & (1 << o)) == 0)
                    continue;
                if (s.Obj[o] == None && (s.Hints & (1 << o)) == 0)
                {
                    State t = s.Clone();
                    t.Obj[o] = Received;
                    yield return ($"SendObject(o{o + 1})", t);
                }

                if (s.Obj[o] == None && (s.Hints & (1 << o)) == 0 && s.HintsUsed < MaxHints)
                {
                    State t = s.Clone();
                    t.Hints |= 1 << o;
                    t.HintsUsed++;
                    yield return ($"FalseInventoryHint(o{o + 1})", t);
                }
            }

            if (s.Manifest[k] == None)
            {
                State t = s.Clone();
                t.Manifest[k] = Received;
                yield return ($"SendManifest({SnapName[k]})", t);
            }

            if (CanPublish(s, k, variant))
            {
                State t = s.Clone();
                t.Published |= 1 << k;
                t.Acked |= 1 << k;
                t.Pointer = k + 1;
                t.Transferring = 0;
                t.Hints = 0;
                yield return ($"Publish({SnapName[k]})", t);
            }
        }

        for (int o = 0; o < Objects; o++)
        {
            if (s.Obj[o] == Received)
            {
                State t = s.Clone();
                t.Obj[o] = Durable;
                yield return ($"FlushObject(o{o + 1})", t);
            }

            if (s.Obj[o] != None && GcMayDelete(s, o, variant))
            {
                State t = s.Clone();
                t.Obj[o] = None;
                yield return ($"BackupGc(o{o + 1})", t);
            }
        }

        for (int k = 0; k < Snapshots; k++)
        {
            int bit = 1 << k;
            if (s.Manifest[k] == Received)
            {
                State t = s.Clone();
                t.Manifest[k] = Durable;
                yield return ($"FlushManifest({SnapName[k]})", t);
            }

            if ((s.Published & bit) != 0 && (s.Expired & bit) == 0)
            {
                State t = s.Clone();
                t.Expired |= bit;
                yield return ($"RetentionElapses({SnapName[k]})", t);
            }

            if ((s.Published & bit) != 0 && (s.Expired & bit) != 0 && s.Pointer != k + 1)
            {
                State t = s.Clone();
                t.Published &= ~bit;
                t.Pruned |= bit;
                t.Manifest[k] = None;
                yield return ($"Prune({SnapName[k]})", t);
            }
        }

        if (s.Crashes < MaxCrashes)
        {
            State t = s.Clone();
            t.Crashes++;
            for (int o = 0; o < Objects; o++)
            {
                if (t.Obj[o] == Received)
                    t.Obj[o] = None;
            }

            for (int k = 0; k < Snapshots; k++)
            {
                if (t.Manifest[k] == Received)
                    t.Manifest[k] = None;
            }

            yield return ("Crash(destination)", t);
        }
    }

    private static bool CanPublish(State s, int k, string variant)
    {
        int needed = variant == PublishBeforeDurable ? Received : Durable;
        if (s.Manifest[k] < needed)
            return false;
        for (int o = 0; o < Objects; o++)
        {
            if ((Deps[k] & (1 << o)) == 0)
                continue;
            bool present = s.Obj[o] >= needed;
            if (variant == PublishTrustingInventoryHints && (s.Hints & (1 << o)) != 0)
                present = true;
            if (!present)
                return false;
        }

        return true;
    }

    private static bool GcMayDelete(State s, int o, string variant)
    {
        int bit = 1 << o;
        if (s.Transferring != 0 && (Deps[s.Transferring - 1] & bit) != 0)
            return false;
        if (variant == GcConsidersLatestSnapshotOnly)
            return s.Pointer == 0 ? s.Published == 0 : (Deps[s.Pointer - 1] & bit) == 0;
        for (int k = 0; k < Snapshots; k++)
        {
            if ((s.Published & (1 << k)) != 0 && (Deps[k] & bit) != 0)
                return false;
        }

        return true;
    }

    protected override string? Invariant(State s, string variant)
    {
        for (int k = 0; k < Snapshots; k++)
        {
            int bit = 1 << k;
            if ((s.Published & bit) != 0)
            {
                if (s.Manifest[k] != Durable)
                    return $"published {SnapName[k]} has no durable manifest";
                for (int o = 0; o < Objects; o++)
                {
                    if ((Deps[k] & (1 << o)) != 0 && s.Obj[o] != Durable)
                        return $"published {SnapName[k]} is incomplete: dependency o{o + 1} is {(s.Obj[o] == None ? "missing" : "not durable")}";
                }
            }

            if ((s.Acked & bit) != 0 && (s.Published & bit) == 0 && !((s.Pruned & bit) != 0 && (s.Expired & bit) != 0))
                return $"acknowledged {SnapName[k]} is no longer retained before its retention expired";
        }

        if (s.Pointer != 0 && (s.Published & (1 << (s.Pointer - 1))) == 0)
            return $"published pointer names {SnapName[s.Pointer - 1]} which is not published";
        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(40);
        ModHelpers.AppendInts(sb, s.Obj);
        sb.Append('m');
        ModHelpers.AppendInts(sb, s.Manifest);
        sb.Append('P').Append(s.Published).Append('A').Append(s.Acked).Append('p').Append(s.Pointer).Append('E').Append(s.Expired)
            .Append('X').Append(s.Pruned).Append('T').Append(s.Transferring).Append('H').Append(s.Hints).Append(s.HintsUsed).Append('c').Append(s.Crashes);
        return sb.ToString();
    }
}
