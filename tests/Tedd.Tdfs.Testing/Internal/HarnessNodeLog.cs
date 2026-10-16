using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Node log sink of a test cluster: keeps the most recent warnings and errors of every node (bounded) for failure reports,
/// and, when the environment variable <c>TDFS_TEST_LOG</c> names a minimum level (<c>trace</c>, <c>debug</c>, <c>info</c>,
/// <c>warn</c>, <c>error</c>), echoes entries at or above it to the console. Thread-safe; never blocks a node loop.
/// </summary>
internal sealed class HarnessNodeLog : INodeLog
{
    private const int Capacity = 200;
    private static readonly NodeLogLevel? EchoLevel = ParseEchoLevel(Environment.GetEnvironmentVariable("TDFS_TEST_LOG"));

    private readonly Queue<string> _recent = new();

    /// <inheritdoc />
    public void Write(NodeLogLevel level, string node, string category, string message)
    {
        if (EchoLevel is { } echo && level >= echo)
            Console.WriteLine($"[{level}] {node} {category}: {message}");
        if (level < NodeLogLevel.Warn)
            return;
        string line = $"[{level}] {node} {category}: {message}";
        lock (_recent)
        {
            _recent.Enqueue(line);
            while (_recent.Count > Capacity)
                _recent.Dequeue();
        }
    }

    /// <summary>The retained warnings and errors, oldest first.</summary>
    public IReadOnlyList<string> Snapshot()
    {
        lock (_recent)
            return _recent.ToArray();
    }

    /// <summary>
    /// A client diagnostics sink that echoes the client's SDK events (with its clock's time) to the console when
    /// <c>TDFS_TEST_LOG</c> is <c>debug</c> or <c>trace</c>; null otherwise.
    /// </summary>
    public static IDfsDiagnostics? ClientEcho(string client, TimeProvider clock) =>
        EchoLevel is { } echo && echo <= NodeLogLevel.Debug ? new ClientEchoSink(client, clock) : null;

    private sealed class ClientEchoSink(string client, TimeProvider clock) : IDfsDiagnostics
    {
        public void OnEvent(DfsDiagnosticEvent e) =>
            Console.WriteLine($"[Client] {clock.GetUtcNow():HH:mm:ss.fffffff} {client} {e.Name}: {e.Message}{(e.Exception is { } ex ? $" ({ex.GetType().Name}: {ex.Message})" : string.Empty)}");
    }

    private static NodeLogLevel? ParseEchoLevel(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : Enum.TryParse(value.Trim(), ignoreCase: true, out NodeLogLevel level) ? level : NodeLogLevel.Info;
}
