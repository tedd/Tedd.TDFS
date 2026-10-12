using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>A configured limit was exceeded (<see cref="DfsErrorCode.LimitExceeded"/>); nothing was applied.</summary>
public class DfsLimitExceededException : DfsException
{
    /// <summary>Creates the exception without limit details.</summary>
    public DfsLimitExceededException(string message, Exception? inner = null)
        : base(DfsErrorCode.LimitExceeded, message, inner)
    {
    }

    /// <summary>Creates the exception for a named limit.</summary>
    /// <param name="limitName">Name of the limit (for example <c>MaxTransactionTouchedInodes</c>).</param>
    /// <param name="limit">The configured limit.</param>
    /// <param name="requested">The requested amount.</param>
    /// <param name="message">Optional message; a default describing the limit is used when null.</param>
    /// <param name="inner">Optional inner exception.</param>
    public DfsLimitExceededException(string limitName, long limit, long requested, string? message = null, Exception? inner = null)
        : base(DfsErrorCode.LimitExceeded, message ?? DefaultMessage(limitName, limit, requested), inner)
    {
        LimitName = limitName ?? string.Empty;
        Limit = limit;
        Requested = requested;
    }

    /// <summary>Name of the exceeded limit (empty when unknown).</summary>
    public string LimitName { get; init; } = string.Empty;

    /// <summary>The configured limit.</summary>
    public long Limit { get; init; }

    /// <summary>The requested amount.</summary>
    public long Requested { get; init; }

    private static string DefaultMessage(string limitName, long limit, long requested)
        => string.Create(CultureInfo.InvariantCulture, $"Limit '{limitName}' exceeded: requested {requested}, limit {limit}.");
}
