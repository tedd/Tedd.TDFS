namespace Tedd.Tdfs;

/// <summary>Options for directory enumeration and paging.</summary>
public sealed record DfsEnumerationOptions
{
    /// <summary>Name pattern with <c>*</c> and <c>?</c> wildcards, matched ordinally.</summary>
    public string Pattern { get; init; } = "*";

    /// <summary>Entry kinds to include.</summary>
    public DfsEntryKinds Kinds { get; init; } = DfsEntryKinds.All;

    /// <summary>Entries per page (1..10000).</summary>
    public int PageSize { get; init; } = 1000;
}
