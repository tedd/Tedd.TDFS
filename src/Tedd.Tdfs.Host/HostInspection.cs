using System.Globalization;
using System.Text;
using System.Text.Json;
using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Host;

/// <summary>
/// Read-only inspection of a launcher-controlled node (<c>--control stdin</c>) for a harness that runs nodes as separate
/// processes and therefore has no in-process inspector: the standard-input line <c>inspect {id} {query} {base64 argument}</c>
/// is answered on standard output with <c>TDFS-INSPECT {id} {base64 JSON}</c> (or <c>TDFS-INSPECT-ERROR {id} {message}</c>).
/// Queries return applied consensus state, a file's layout and verified local copies — the same ground truth the in-process
/// harness reads through <see cref="NodeInspector"/>; never payload bytes or secrets (SEC-016).
/// </summary>
/// <remarks>
/// Queries: <c>group</c> (argument: group name; <see cref="GroupInspection"/> or null), <c>groups</c> (every hosted group),
/// <c>file</c> (argument:
/// <c>volume</c> and normalized path separated by a line feed; <see cref="InspectedFile"/> or null when the volume or file
/// is not hosted/present), <c>verify</c> (argument: object ids as decimal 128-bit values separated by commas; the ids of
/// the copies this node holds and re-read successfully), <c>digest</c> (argument: group name; the replicated-state digest
/// line or null).
/// </remarks>
public static class HostInspection
{
    /// <summary>Prefix of an answer line.</summary>
    public const string ReplyPrefix = "TDFS-INSPECT ";

    /// <summary>Prefix of a failure line.</summary>
    public const string ErrorPrefix = "TDFS-INSPECT-ERROR ";

    /// <summary>JSON options of the answers.</summary>
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);

    /// <summary>Answers one query against <paramref name="inspector"/> (JSON text).</summary>
    public static async Task<string> InvokeAsync(NodeInspector inspector, string query, string argument)
    {
        ArgumentNullException.ThrowIfNull(inspector);
        switch (query)
        {
            case "group":
                return JsonSerializer.Serialize(await inspector.GetGroupAsync(argument), Options);
            case "groups":
                return JsonSerializer.Serialize(await inspector.GetGroupsAsync(), Options);
            case "file":
            {
                int split = argument.IndexOf('\n', StringComparison.Ordinal);
                if (split < 0)
                    throw new ArgumentException("The file query needs 'volume\\npath'.");
                VolumeStateInspection? state = await inspector.GetVolumeStateAsync(argument[..split]);
                FileInspection? file = state?.GetFile(argument[(split + 1)..]);
                return JsonSerializer.Serialize(file is null ? null : InspectedFile.From(file), Options);
            }

            case "verify":
            {
                ObjectId[] ids = argument.Length == 0
                    ? []
                    : [.. argument.Split(',').Select(v => new ObjectId(UInt128.Parse(v, CultureInfo.InvariantCulture)))];
                IReadOnlySet<ObjectId> verified = await inspector.VerifyObjectsAsync(ids);
                return JsonSerializer.Serialize(verified.Select(o => o.Value.ToString(CultureInfo.InvariantCulture)).Order(StringComparer.Ordinal).ToArray(), Options);
            }

            case "digest":
                return JsonSerializer.Serialize(await inspector.ComputeGroupDigestAsync(argument), Options);
            default:
                throw new ArgumentException($"Unknown inspection query '{query}' (expected group, groups, file, verify or digest).");
        }
    }

    /// <summary>Decodes a base64 UTF-8 argument.</summary>
    public static string DecodeArgument(string base64) => base64.Length == 0 ? string.Empty : Encoding.UTF8.GetString(Convert.FromBase64String(base64));

    /// <summary>Encodes an answer for one output line.</summary>
    public static string EncodeAnswer(string json) => Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
}

/// <summary>A file's layout as answered by the <c>file</c> inspection query (object ids as decimal 128-bit values).</summary>
/// <param name="Path">Normalized path.</param>
/// <param name="Inode">Inode id.</param>
/// <param name="Revision">Revision.</param>
/// <param name="Length">Length.</param>
/// <param name="IsInline">True when stored inline.</param>
/// <param name="WriterGeneration">Writer generation.</param>
/// <param name="Policy">Storage policy name.</param>
/// <param name="Extents">Extents of the default stream.</param>
public sealed record InspectedFile(string Path, ulong Inode, ulong Revision, long Length, bool IsInline, ulong WriterGeneration, string Policy,
    IReadOnlyList<InspectedExtent> Extents)
{
    internal static InspectedFile From(FileInspection file) => new(file.Path, file.Inode.Value, file.Revision, file.Length, file.IsInline,
        file.WriterGeneration, file.Policy,
        [.. file.Extents.Select(e => new InspectedExtent(e.Offset, e.Length, e.Object.Value.ToString(CultureInfo.InvariantCulture), e.ObjectOffset, e.ObjectLength))]);

    /// <summary>The inspection record this answer describes.</summary>
    public FileInspection ToInspection() => new(Path, new InodeId(Inode), Revision, Length, IsInline, WriterGeneration, Policy,
        [.. Extents.Select(e => new ExtentInspection(e.Offset, e.Length, new ObjectId(UInt128.Parse(e.Object, CultureInfo.InvariantCulture)), e.ObjectOffset, e.ObjectLength))]);
}

/// <summary>One extent of an <see cref="InspectedFile"/>.</summary>
/// <param name="Offset">File offset.</param>
/// <param name="Length">Length.</param>
/// <param name="Object">Object id (decimal 128-bit value).</param>
/// <param name="ObjectOffset">Offset in the object.</param>
/// <param name="ObjectLength">Object length.</param>
public sealed record InspectedExtent(long Offset, long Length, string Object, long ObjectOffset, long ObjectLength);
