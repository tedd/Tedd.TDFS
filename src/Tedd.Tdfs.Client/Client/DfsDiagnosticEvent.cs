namespace Tedd.Tdfs;

/// <summary>An SDK diagnostic event. Never carries file payloads or credentials (SDK-016).</summary>
/// <param name="Name">Event name, e.g. <c>connect.seed-failed</c>.</param>
/// <param name="OperationId">Related operation id, if any.</param>
/// <param name="TransactionId">Related transaction id, if any.</param>
/// <param name="Volume">Related volume name, if any.</param>
/// <param name="Token">Related commit token, if any.</param>
/// <param name="Message">Human-readable description.</param>
/// <param name="Exception">Related exception, if any.</param>
public sealed record DfsDiagnosticEvent(
    string Name,
    DfsOperationId? OperationId,
    DfsTransactionId? TransactionId,
    string? Volume,
    CommitToken? Token,
    string Message,
    Exception? Exception);
