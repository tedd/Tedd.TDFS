using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-010: source partition, explicit restore/promotion of a backup under the same volume identity with a new authority
/// epoch, and return of the old source (FSD §12.5, ARCHITECTURE §12, RST tests). The old authority accepts writes only
/// while it holds an authority lease from the cluster registry; promotion waits until that lease has expired at the
/// registry and stops renewals.
/// </summary>
internal sealed class RecoveryEpochSpec : ProtocolSpec<RecoveryEpochSpec.State>
{
    private const int MaxToggles = 2;
    private const int MaxOldWrites = 2;

    /// <summary>Promotion does not wait for the old authority's lease to lapse.</summary>
    public const string PromoteWithoutFencing = "PromoteWithoutFencingOldAuthority";

    /// <summary>A network partition automatically promotes the backup (forbidden by FSD §12.5).</summary>
    public const string AutomaticPromotionOnPartition = "AutomaticPromotionOnPartition";

    /// <summary>The returning old source renews its lease without consulting the registry epoch.</summary>
    public const string OldSourceRenewsAfterReturn = "OldSourceSelfRenewsAfterReturn";

    internal sealed class State
    {
        public bool OldAlive = true;
        public bool Partitioned;
        public bool HolderLease = true;     // the old authority's own view of its lease
        public bool RegistryLease = true;   // the registry's view of the old authority's lease
        public bool Restoring;              // explicit admin promotion started: renewals refused
        public bool Promoted;               // backup promoted under the same identity, epoch + 1
        public bool OldFenced;              // old authority learned it was deposed
        public bool BackupComplete;         // a complete published snapshot is selected
        public bool RestoredAsNewVolume;    // default restore: new identity (never conflicts)
        public int OldWrites;
        public int NewWrites;
        public int Partitions;
        public int Crashes;
        public string? Violation;           // ghost

        public State Clone() => (State)MemberwiseClone();
    }

    public override string TestId => "MOD-010";
    public override string Name => "RecoveryEpochModel";

    public override string Summary =>
        "One volume identity V. The old source authority (epoch e) accepts writes only while its registry-granted authority " +
        "lease is valid in its own view. The source may be partitioned from the registry/admin (but not from its own clients), " +
        "crash and return. An administrator may restore into a new volume (new identity) or explicitly promote a complete " +
        "backup snapshot to identity V with epoch e+1; promotion first stops renewals and waits until the registry considers " +
        "the old lease expired. A returning old source that contacts the registry learns it was deposed.";

    public override SpecBounds Bounds { get; } = new(48, 1_000_000,
        [$"partitions<={MaxToggles}", $"old-source crashes<={MaxToggles}", $"old-authority writes<={MaxOldWrites}", "promoted-authority writes<=1"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "No two authorities accept writes under the same recovered identity: after promotion the old authority never accepts a write for V.",
        "At no instant can both the promoted authority and the old authority accept writes for V.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Bounded clock drift: the old authority's lease expires in its own view no later than in the registry's view (RegistryTick requires the holder to have expired).",
        "Promotion is an explicit administrative action (never automatic on partition, FSD §12.5).",
        "The registry (control group) is itself a consensus group with a single authoritative view of V's epoch.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Lease time is abstracted into Valid/Expired transitions with the drift assumption above.",
        "Client tokens are abstracted: any client may reach whichever authority is alive on its side of the partition.",
        "Data contents and backup completeness are abstracted into one 'complete snapshot selected' flag (MOD-007 covers completeness).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'Partition', 'Heal', 'Crash', 'Return', 'HolderTick', 'RegistryTick', 'CompleteSnapshot', 'BeginPromotion', 'RestoreAsNewVolume' and 'WriteNew' have no dedicated fault site (TestNetwork, TestNode, TestClock and admin calls drive them in RST tests).",
        "The implementation's mechanism that stops an old volume group from accepting writes after promotion (lease or epoch check against the control group) must be shown to refine 'HolderLease'; this is not proved here (RST-008/RST-009).",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Partition", []),
        ("Heal", []),
        ("Crash", []),
        ("Return", [FaultSites.RecoveryJournalReplay, FaultSites.RecoveryCheckpointInstall]),
        ("HolderTick", []),
        ("RegistryTick", []),
        ("Renew", [FaultSites.Reconfiguration]),
        ("CompleteSnapshot", []),
        ("BeginPromotion", []),
        ("Promote", [FaultSites.Reconfiguration]),
        ("RestoreAsNewVolume", []),
        ("WriteOld", [FaultSites.RaftBeforePropose, FaultSites.ApplyAfterBeforeReply]),
        ("WriteNew", []));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(PromoteWithoutFencing, "promotion proceeds while the old authority's lease may still be valid"),
        new(AutomaticPromotionOnPartition, "a partition automatically promotes the backup"),
        new(OldSourceRenewsAfterReturn, "a returning old source renews its own lease without the registry epoch check"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        if (!s.Partitioned && s.Partitions < MaxToggles)
        {
            State t = s.Clone();
            t.Partitioned = true;
            t.Partitions++;
            yield return ("Partition", t);
            if (variant == AutomaticPromotionOnPartition && s.BackupComplete && !s.Promoted)
            {
                State auto = t.Clone();
                auto.Promoted = true;
                yield return ("Promote(automatic)", auto);
            }
        }

        if (s.Partitioned)
        {
            State t = s.Clone();
            t.Partitioned = false;
            yield return ("Heal", t);
        }

        if (s.OldAlive && s.Crashes < MaxToggles)
        {
            State t = s.Clone();
            t.OldAlive = false;
            t.HolderLease = false;
            t.Crashes++;
            yield return ("Crash(old)", t);
        }

        if (!s.OldAlive)
        {
            State t = s.Clone();
            t.OldAlive = true;
            t.HolderLease = variant == OldSourceRenewsAfterReturn && !s.OldFenced;
            yield return ("Return(old)", t);
        }

        if (s.HolderLease)
        {
            State t = s.Clone();
            t.HolderLease = false;
            yield return ("HolderTick", t);
        }

        if (s.RegistryLease && !s.HolderLease)
        {
            State t = s.Clone();
            t.RegistryLease = false;
            yield return ("RegistryTick", t);
        }

        if (s.OldAlive && !s.Partitioned && !s.OldFenced)
        {
            State t = s.Clone();
            if (s.Promoted)
            {
                t.OldFenced = true;
                t.HolderLease = false;
                yield return ("Renew(rejected:deposed)", t);
            }
            else if (!s.Restoring && !(s.HolderLease && s.RegistryLease))
            {
                t.HolderLease = true;
                t.RegistryLease = true;
                yield return ("Renew(granted)", t);
            }
        }

        if (!s.BackupComplete)
        {
            State t = s.Clone();
            t.BackupComplete = true;
            yield return ("CompleteSnapshot", t);
        }

        if (!s.Restoring && !s.Promoted)
        {
            State t = s.Clone();
            t.Restoring = true;
            yield return ("BeginPromotion", t);
        }

        if (s.Restoring && !s.Promoted && s.BackupComplete && (variant == PromoteWithoutFencing || !s.RegistryLease))
        {
            State t = s.Clone();
            t.Promoted = true;
            yield return ("Promote(explicit)", t);
        }

        if (s.BackupComplete && !s.RestoredAsNewVolume)
        {
            State t = s.Clone();
            t.RestoredAsNewVolume = true;
            yield return ("RestoreAsNewVolume", t);
        }

        if (OldCanAccept(s) && s.OldWrites < MaxOldWrites)
        {
            State t = s.Clone();
            t.OldWrites++;
            if (s.Promoted)
                t.Violation ??= "the old authority accepted a write for the recovered identity after the backup was promoted";
            yield return ("WriteOld", t);
        }

        if (s.Promoted && s.NewWrites < 1)
        {
            State t = s.Clone();
            t.NewWrites++;
            yield return ("WriteNew", t);
        }
    }

    private static bool OldCanAccept(State s) => s.OldAlive && !s.OldFenced && s.HolderLease;

    protected override string? Invariant(State s, string variant)
    {
        if (s.Violation is not null)
            return "split authority: " + s.Violation;
        if (s.Promoted && OldCanAccept(s))
            return "split authority: the promoted backup and the old source can both accept writes for the same identity";
        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(32);
        ModHelpers.AppendBools(sb, [s.OldAlive, s.Partitioned, s.HolderLease, s.RegistryLease, s.Restoring, s.Promoted, s.OldFenced, s.BackupComplete, s.RestoredAsNewVolume]);
        sb.Append(s.OldWrites).Append(s.NewWrites).Append(s.Partitions).Append(s.Crashes).Append(s.Violation is null ? '.' : '!');
        return sb.ToString();
    }
}
