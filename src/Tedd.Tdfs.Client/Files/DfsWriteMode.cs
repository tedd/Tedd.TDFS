namespace Tedd.Tdfs;

/// <summary>When writes on a stream are published.</summary>
public enum DfsWriteMode
{
    /// <summary>Writes are staged privately and published by Flush, Commit or Dispose.</summary>
    OnFlush,

    /// <summary>Each write call is published durably before it returns.</summary>
    WriteThrough,
}
