namespace Tedd.Tdfs;

/// <summary>Application hook that makes a snapshot application-consistent (FSD §11.3).</summary>
public interface IDfsSnapshotAdapter
{
    /// <summary>Stable adapter identifier recorded in the snapshot.</summary>
    string Id { get; }

    /// <summary>Adapter version recorded in the snapshot.</summary>
    string Version { get; }

    /// <summary>Requirements a restore must satisfy, recorded in the snapshot.</summary>
    IReadOnlyDictionary<string, string> RestoreRequirements { get; }

    /// <summary>Checkpoints or pauses the application before capture.</summary>
    Task PrepareAsync(DfsSnapshotAdapterContext context, CancellationToken ct);

    /// <summary>Resumes the application; always attempted after a successful prepare.</summary>
    Task ResumeAsync(DfsSnapshotAdapterContext context, CancellationToken ct);

    /// <summary>Called when capture fails or times out.</summary>
    Task AbortAsync(DfsSnapshotAdapterContext context, CancellationToken ct);
}
