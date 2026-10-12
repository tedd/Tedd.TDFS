namespace Tedd.Tdfs;

/// <summary>When a backup target is synchronised automatically.</summary>
/// <param name="Interval">Periodic synchronisation interval; null disables periodic sync.</param>
/// <param name="OnEverySnapshot">Synchronise after every snapshot.</param>
public sealed record DfsBackupSchedule(TimeSpan? Interval = null, bool OnEverySnapshot = false);
