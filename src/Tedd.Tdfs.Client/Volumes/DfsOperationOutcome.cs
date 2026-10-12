namespace Tedd.Tdfs;

/// <summary>Result of an operation-outcome query.</summary>
/// <param name="Id">The queried operation id.</param>
/// <param name="Status">Outcome status.</param>
/// <param name="Token">Commit token when <see cref="DfsOutcomeStatus.Committed"/>; otherwise <see cref="CommitToken.None"/>.</param>
/// <param name="Error">Error code of an aborted operation; <see cref="DfsErrorCode.None"/> otherwise.</param>
/// <param name="Detail">Optional detail text.</param>
public sealed record DfsOperationOutcome(DfsOperationId Id, DfsOutcomeStatus Status, CommitToken Token, DfsErrorCode Error, string? Detail);
