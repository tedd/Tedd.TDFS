using System.Collections.Immutable;
using System.Globalization;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    private const string TransferBindingPrefix = "created:";
    private const string TransferPinnedSeparator = ";pinned:";

    /// <summary>
    /// The binding recorded on a transfer pin: the creation time of the pinned snapshot, so every leader describes the same
    /// transfer identically even after the source snapshot record was deleted (the pin keeps its root, FSD §11.1), and the
    /// leader time the pin was taken, so every leader measures the pin's age budget from the same start (FSD §12.4).
    /// </summary>
    internal static string TransferBinding(DateTimeOffset createdUtc, DateTimeOffset pinnedUtc)
        => TransferBindingPrefix + createdUtc.UtcTicks.ToString(CultureInfo.InvariantCulture)
           + TransferPinnedSeparator + pinnedUtc.UtcTicks.ToString(CultureInfo.InvariantCulture);

    /// <summary>The snapshot creation time recorded on a transfer pin (null for pins without one).</summary>
    internal static DateTimeOffset? TransferCreatedUtc(PinRecord pin)
    {
        ArgumentNullException.ThrowIfNull(pin);
        if (!pin.Binding.StartsWith(TransferBindingPrefix, StringComparison.Ordinal))
            return null;
        ReadOnlySpan<char> value = pin.Binding.AsSpan(TransferBindingPrefix.Length);
        int separator = value.IndexOf(TransferPinnedSeparator, StringComparison.Ordinal);
        return ParseUtcTicks(separator < 0 ? value : value[..separator]);
    }

    /// <summary>The leader time a transfer pin was taken (null for pins recorded without one).</summary>
    internal static DateTimeOffset? TransferPinnedUtc(PinRecord pin)
    {
        ArgumentNullException.ThrowIfNull(pin);
        if (!pin.Binding.StartsWith(TransferBindingPrefix, StringComparison.Ordinal))
            return null;
        ReadOnlySpan<char> value = pin.Binding.AsSpan(TransferBindingPrefix.Length);
        int separator = value.IndexOf(TransferPinnedSeparator, StringComparison.Ordinal);
        return separator < 0 ? null : ParseUtcTicks(value[(separator + TransferPinnedSeparator.Length)..]);
    }

    private static DateTimeOffset? ParseUtcTicks(ReadOnlySpan<char> text)
    {
        if (!long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out long ticks)
            || ticks < DateTimeOffset.MinValue.UtcTicks || ticks > DateTimeOffset.MaxValue.UtcTicks)
        {
            return null;
        }

        return new DateTimeOffset(ticks, TimeSpan.Zero);
    }

    private static (VolumeState, VolumeApplyResult) ApplyRequestBackupSync(VolumeState s, ApplyContext ctx, RequestBackupSyncCommand c)
    {
        if (ctx.Op is { } op && !op.IsAdmin && (op.Grants & DfsPermissions.BackupAdmin) == 0)
            throw Fail(DfsErrorCode.AccessDenied, $"Principal '{op.Principal}' lacks the BackupAdmin grant on this volume (request backup sync).");
        if (string.IsNullOrEmpty(c.Target))
            throw Fail(DfsErrorCode.InvalidArgument, "A backup target name is required.");

        string name;
        ulong boundary;
        DateTimeOffset created;
        ImmutableSortedDictionary<ulong, InodeRecord> root;
        if (c.Snapshot is { } snapshotName)
        {
            if (!s.Snapshots.TryGetValue(snapshotName, out SnapshotRecord? snapshot))
                throw Fail(DfsErrorCode.SnapshotNotFound, $"Snapshot '{snapshotName}' does not exist.");
            name = snapshot.Name;
            boundary = snapshot.BoundaryIndex;
            created = snapshot.CreatedUtc;
            root = snapshot.Root;
        }
        else
        {
            string exportName = c.ExportName ?? throw Fail(DfsErrorCode.InvalidArgument, "An export snapshot needs a name.");
            if (SnapshotNames.GetError(exportName) is { } error)
                throw Fail(DfsErrorCode.InvalidArgument, error);
            name = exportName;
            boundary = ctx.Index;
            created = ctx.Now;
            root = s.Inodes;
        }

        PinRecord? pin = s.Pins.Values.FirstOrDefault(p => p.Kind == PinKind.Transfer && string.Equals(p.Target, c.Target, StringComparison.Ordinal)
            && p.PinIndex == boundary && string.Equals(p.Snapshot, name, StringComparison.Ordinal));
        if (pin is null)
            (s, pin) = AddPin(s, PinKind.Transfer, 0, string.Empty, ctx.Index, boundary, 0, name, 0, TransferBinding(created, ctx.Now), root, c.Target);
        return (s, VolumeApplyResult.Ok(new BackupRequestSyncResultMessage(true, s.TokenAt(boundary)), pin));
    }

    private static (VolumeState, VolumeApplyResult) ApplyRemoveBackupTargetState(VolumeState s, ApplyContext ctx, RemoveBackupTargetStateCommand c)
    {
        RequireGrant(ctx, DfsPermissions.BackupAdmin, "remove backup target");
        int released = 0;
        foreach (PinRecord pin in s.Pins.Values.Where(p => p.Kind == PinKind.Transfer && string.Equals(p.Target, c.Target, StringComparison.Ordinal)).ToArray())
        {
            s = s with { Pins = s.Pins.Remove(pin.Id) };
            released++;
        }

        s = s with { BackupProgress = s.BackupProgress.Remove(c.Target) };
        return (s, VolumeApplyResult.Ok(null, released));
    }
}
