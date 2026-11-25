using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Volumes.Commands;

internal sealed record ExportOpenCommand(ExportOpenMessage Request, TimeSpan Lease) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ExportOpen;
    internal override void WriteBody(ref WireWriter w)
    {
        Request.Write(ref w);
        w.WriteTimeSpan(Lease);
    }
    internal static ExportOpenCommand ReadBody(ref WireReader r) => new(ExportOpenMessage.Read(ref r), r.ReadTimeSpan());
}

internal sealed record ExportControlCommand(ExportControlMessage Request) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ExportControl;
    internal override void WriteBody(ref WireWriter w) => Request.Write(ref w);
    internal static ExportControlCommand ReadBody(ref WireReader r) => new(ExportControlMessage.Read(ref r));
}
