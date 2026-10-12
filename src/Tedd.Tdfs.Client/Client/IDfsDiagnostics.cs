namespace Tedd.Tdfs;

/// <summary>Receives SDK diagnostic events. Events never contain payload bytes or secrets (SDK-016).</summary>
public interface IDfsDiagnostics
{
    /// <summary>Called for each diagnostic event. Implementations must not block or throw.</summary>
    void OnEvent(DfsDiagnosticEvent e);
}
