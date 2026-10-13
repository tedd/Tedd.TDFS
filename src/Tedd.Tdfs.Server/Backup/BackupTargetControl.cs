using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Crypto;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Control-leader side of backup target registration (<c>BackupTargetUpdate</c>, PROTOCOL.md §8.6): re-checks the requesting
/// principal's grant on the authoritative control state and commits <see cref="RegisterBackupTargetCommand"/> or
/// <see cref="RemoveBackupTargetCommand"/> under the forwarded request id, so a retried registration (same operation id) is
/// deduplicated by the control group's administrative outcomes. Also owns the wrapping of export secrets: the volume leader
/// wraps the export principal's secret with a key derived from the cluster key-encryption key before it reaches any log.
/// </summary>
internal sealed class BackupTargetControl
{
    private const string WrapPurpose = "backup-export-secret";
    private const int NonceSize = 12;
    private const int TagSize = 16;

    private readonly NodeServices _s;

    public BackupTargetControl(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>The <c>BackupTargetUpdate</c> handler (node peers only).</summary>
    public async ValueTask<IWireMessage> HandleAsync(RpcContext context, IWireMessage request)
        => await ExecuteAsync((BackupTargetUpdateMessage)request, context.Cancellation);

    /// <summary>Applies one registration change on the control leader.</summary>
    public async Task<BackupTargetUpdateResultMessage> ExecuteAsync(BackupTargetUpdateMessage message, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(message);
        ControlHost control = _s.Control;
        if (!control.IsLeader)
            throw control.NotLeader();
        if (message.RequestId == UInt128.Zero)
            throw new ArgumentException("A backup target change needs a request id.");
        if (string.IsNullOrEmpty(message.Target))
            throw new ArgumentException("A backup target name is required.");
        await control.Group!.ReadIndexAsync(ct);
        ControlState state = control.State;
        if (state.AdminOutcomes.TryGetValue(message.RequestId, out AdminOutcome? prior))
        {
            if (prior.Code != DfsErrorCode.None)
                throw DfsErrors.ToException(prior.Code, prior.Message);
            return Reply(state.FindVolume(message.Volume), prior.Index);
        }

        VolumeRecord volume = state.FindVolume(message.Volume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {message.Volume} does not exist.");
        if (!state.IsClusterAdmin(message.Principal) && (state.GetPermissions(message.Principal, volume.Id) & DfsPermissions.BackupAdmin) == 0)
            throw new UnauthorizedAccessException($"Principal '{message.Principal}' lacks the BackupAdmin grant on volume '{volume.Name}'.");

        ControlCommand command;
        switch (message.Action)
        {
            case BackupTargetAction.Register:
            case BackupTargetAction.Configure:
            {
                if (message.Action == BackupTargetAction.Configure && !volume.BackupTargets.ContainsKey(message.Target))
                    throw new DfsException(DfsErrorCode.BackupTargetNotFound, $"Backup target '{message.Target}' is not registered on volume '{volume.Name}'.");
                if (message.Endpoints.Count == 0 || message.Endpoints.Any(string.IsNullOrEmpty) || string.IsNullOrEmpty(message.ExportPrincipal)
                    || message.WrappedExportSecret.IsEmpty || message.Schedule is null || message.Budgets is null)
                {
                    throw new ArgumentException("A backup target registration needs endpoints, export credentials, a schedule and budgets.");
                }

                BackupBudgetsData budgets = message.Budgets;
                var record = new BackupTargetRecord(message.Target, [.. message.Endpoints], message.ExportPrincipal, message.WrappedExportSecret.ToArray(),
                    message.Schedule.Interval, message.Schedule.OnEverySnapshot, budgets.MaxPinAge, budgets.MaxPinnedBytes, budgets.MaxBytesPerSecond,
                    budgets.MaxConcurrentObjects, 0);
                command = new RegisterBackupTargetCommand(control.NewHeader(message.RequestId, message.Principal), volume.Id, record);
                break;
            }

            case BackupTargetAction.Remove:
                command = new RemoveBackupTargetCommand(control.NewHeader(message.RequestId, message.Principal), volume.Id, message.Target);
                break;

            default:
                throw new ArgumentException($"Backup target action {message.Action} does not change the registration.");
        }

        ApplyOutcome outcome = await control.ProposeAsync(command, ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        return Reply(control.State.FindVolume(message.Volume), outcome.Index);
    }

    private static BackupTargetUpdateResultMessage Reply(VolumeRecord? volume, ulong index)
        => new(volume is null ? [] : [.. volume.BackupTargets.Keys.Order(StringComparer.Ordinal)], index);

    // ------------------------------------------------------------------------------------------------- export secrets

    /// <summary>Wraps an export principal's secret for the control record (AES-256-GCM under a key derived from the cluster KEK).</summary>
    public static byte[] WrapExportSecret(byte[] storageKek, VolumeId volume, string target, string secret, IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(storageKek);
        ArgumentNullException.ThrowIfNull(secret);
        byte[] key = KeyDerivation.DeriveKey(storageKek, WrapPurpose);
        byte[] plain = Encoding.UTF8.GetBytes(secret);
        var output = new byte[NonceSize + plain.Length + TagSize];
        random.Fill(output.AsSpan(0, NonceSize));
        try
        {
            using var aes = new AesGcm(key, TagSize);
            aes.Encrypt(output.AsSpan(0, NonceSize), plain, output.AsSpan(NonceSize, plain.Length), output.AsSpan(NonceSize + plain.Length), Aad(volume, target));
            return output;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(plain);
        }
    }

    /// <summary>Unwraps an export secret; a foreign or tampered value fails with <see cref="DfsIntegrityException"/>.</summary>
    public static string UnwrapExportSecret(byte[] storageKek, VolumeId volume, string target, ReadOnlySpan<byte> wrapped)
    {
        ArgumentNullException.ThrowIfNull(storageKek);
        if (wrapped.Length < NonceSize + TagSize)
            throw new DfsIntegrityException($"The export secret of backup target '{target}' is truncated.");
        byte[] key = KeyDerivation.DeriveKey(storageKek, WrapPurpose);
        var plain = new byte[wrapped.Length - NonceSize - TagSize];
        try
        {
            using var aes = new AesGcm(key, TagSize);
            aes.Decrypt(wrapped[..NonceSize], wrapped.Slice(NonceSize, plain.Length), wrapped[(NonceSize + plain.Length)..], plain, Aad(volume, target));
            return Encoding.UTF8.GetString(plain);
        }
        catch (AuthenticationTagMismatchException ex)
        {
            throw new DfsIntegrityException($"The export secret of backup target '{target}' cannot be unwrapped with this cluster's key material.", ex);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(plain);
        }
    }

    private static byte[] Aad(VolumeId volume, string target) => Encoding.UTF8.GetBytes("tdfs/backup-export-secret/v1\0" + volume.Value.ToString("N") + "\0" + target);
}
