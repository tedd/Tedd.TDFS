namespace Tedd.Tdfs;

/// <summary>Registration of a late-sync backup target for a volume.</summary>
/// <param name="Name">Target name (unique per volume).</param>
/// <param name="Endpoints">Backup node endpoints.</param>
/// <param name="ExportCredentials">Credentials the exporter uses on the backup node (no retention rights).</param>
/// <param name="Schedule">Synchronisation schedule.</param>
/// <param name="Budgets">Source-side budgets.</param>
public sealed record DfsBackupTargetRegistration(
    string Name,
    IReadOnlyList<string> Endpoints,
    DfsCredentials ExportCredentials,
    DfsBackupSchedule Schedule,
    DfsBackupBudgets Budgets);
