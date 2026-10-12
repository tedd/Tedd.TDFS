using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Ops;

/// <summary>Shared helpers for the OPS unit tests.</summary>
internal static class OpsHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static byte[] Concat(byte[] a, byte[] b)
    {
        var result = new byte[a.Length + b.Length];
        a.CopyTo(result, 0);
        b.CopyTo(result, a.Length);
        return result;
    }

    /// <summary>Byte-exact comparison taking arrays, so call sites may await inside the argument list.</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);
}

/// <summary>Thread-safe <see cref="IDfsDiagnostics"/> sink that records every SDK diagnostic event in order.</summary>
internal sealed class DiagnosticsRecorder : IDfsDiagnostics
{
    private readonly System.Threading.Lock _lock = new();
    private readonly List<DfsDiagnosticEvent> _events = [];

    /// <summary>Number of events recorded so far (use as a mark).</summary>
    public int Count
    {
        get
        {
            lock (_lock)
                return _events.Count;
        }
    }

    /// <inheritdoc />
    public void OnEvent(DfsDiagnosticEvent e)
    {
        ArgumentNullException.ThrowIfNull(e);
        lock (_lock)
            _events.Add(e);
    }

    /// <summary>Events recorded at or after <paramref name="mark"/>.</summary>
    public IReadOnlyList<DfsDiagnosticEvent> Since(int mark)
    {
        lock (_lock)
            return _events.Skip(mark).ToArray();
    }
}
