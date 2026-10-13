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

    private static NodeLogLevel? ParseEchoLevel(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : Enum.TryParse(value.Trim(), ignoreCase: true, out NodeLogLevel level) ? level : NodeLogLevel.Info;
}
