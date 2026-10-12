namespace Tedd.Tdfs;

/// <summary>Base type of distributed-filesystem failures that are surfaced as <see cref="IOException"/>.</summary>
public class DfsException : IOException
{
    private bool _isRetryable;
    private DfsOperationId? _operationId;

    /// <summary>Creates an exception with an error code.</summary>
    public DfsException(DfsErrorCode code, string message, Exception? inner = null)
        : base(message, inner)
    {
        ErrorCode = code;
    }

    /// <summary>The canonical error code.</summary>
    public DfsErrorCode ErrorCode { get; }

    /// <summary>True when retrying the same operation (with the same operation id) may succeed.</summary>
    public bool IsRetryable
    {
        get => _isRetryable;
        init => _isRetryable = value;
    }

    /// <summary>Operation id of the failed operation, when known.</summary>
    public DfsOperationId? OperationId
    {
        get => _operationId;
        init => _operationId = value;
    }

    /// <summary>Overrides retryability after construction (used by <see cref="DfsErrors"/> when decoding).</summary>
    internal void SetRetryable(bool value) => _isRetryable = value;

    /// <summary>Overrides the operation id after construction (used by <see cref="DfsErrors"/> when decoding).</summary>
    internal void SetOperationId(DfsOperationId? value) => _operationId = value;
}
