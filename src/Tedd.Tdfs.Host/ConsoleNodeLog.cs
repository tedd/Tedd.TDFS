using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Host;

/// <summary>Writes node log entries at or above a level to standard error (standard output carries the control lines).</summary>
internal sealed class ConsoleNodeLog : INodeLog
{
    private readonly NodeLogLevel _minimum;
    private readonly Lock _gate = new();

    public ConsoleNodeLog(NodeLogLevel minimum)
    {
        _minimum = minimum;
    }

    /// <inheritdoc />
    public void Write(NodeLogLevel level, string node, string category, string message)
    {
        if (level < _minimum)
            return;
        string line = $"{DateTimeOffset.UtcNow:O} [{level}] {node} {category}: {message}";
        lock (_gate)
            Console.Error.WriteLine(line);
    }
}
