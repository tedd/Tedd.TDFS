using System.Buffers;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Raft;

internal sealed partial class GroupRuntime
{
    private const int CheckpointChunkBytes = 1 << 20;

    /// <summary>The latest checkpoint image held in memory (served to lagging followers).</summary>
    public RaftSnapshot? LatestSnapshot => _latestSnapshot;

    /// <summary>
    /// Follower side of a <c>RaftSnapshotChunk</c> request: reports the <see cref="ResyncDetails.Receive"/> fault stage, steps
    /// the chunk into the Raft core, processes the resulting Ready and returns the core's chunk acknowledgement (null when the
    /// core answered differently, for example with an append acknowledgement after installing the final chunk).
    /// </summary>
    public async Task<InstallSnapshotResponse?> StepSnapshotChunkAsync(InstallSnapshot chunk, CancellationToken ct = default)
    {
        ThrowIfUnavailable();
        int chunkNumber = chunk.Offset <= 0 ? 1 : (int)Math.Min(int.MaxValue, (chunk.Offset / SnapshotChunkBytes) + 1);
        await _s.HitAsync(FaultStage.F16, FaultSites.RaftSnapshotInstall, VolumeName, DfsOperationKind.RaftSnapshot,
            occurrence: chunkNumber, detail: ResyncDetails.Receive, ct: ct);
        ThrowIfUnavailable();
        if (chunk.Meta.Index <= _node.CommitIndex)
            Stats.SnapshotsRejectedStale++;
        var capture = new SnapshotChunkCapture(chunk.From, chunk.Meta.Index);
        _chunkCaptures.Add(capture);
        try
        {
            Step(chunk);
            await WaitIdleAsync();
            return capture.Response;
        }
        finally
        {
            _chunkCaptures.Remove(capture);
        }
    }

    /// <summary>
    /// Applied index after which the entries replayed from the local journal at start are checkpointed (null when there was
    /// nothing to replay or the recovery checkpoint was written).
    /// </summary>
    private ulong? _recoveryCheckpointAt;

    /// <summary>
    /// Writes a local checkpoint and compacts the log once enough entries were applied since the last one, and once after
    /// the local replay of a restart.
    /// </summary>
    private async Task MaybeCheckpointAsync()
    {
        if (_recoveryCheckpointAt is { } replayed && _appliedIndex >= replayed)
        {
            _recoveryCheckpointAt = null;
            await WriteCheckpointAsync();
            return;
        }

        ulong snapshotIndex = _view.Snapshot.Index;
        if (_appliedIndex <= snapshotIndex || _appliedIndex - snapshotIndex <= (ulong)_s.Timings.RaftSnapshotThreshold)
            return;
        await WriteCheckpointAsync();
    }

    /// <summary>Checkpoints the state at the applied index and compacts the log through it (checkpoint first: crash-safe).</summary>
    public async Task WriteCheckpointAsync()
    {
        ulong index = _appliedIndex;
        if (index == 0 || index <= _view.Snapshot.Index || (_checkpoints.Current is { } current && current.Index >= index))
            return;
        var buffer = new ArrayBufferWriter<byte>();
        _sm.WriteCheckpoint(buffer);
        var meta = new RaftSnapshotMeta(index, _appliedTerm, _appliedConf);
        byte[] envelope = SnapshotEnvelope.Encode(meta, buffer.WrittenSpan);
        try
        {
            await SaveCheckpointAsync(meta, envelope);
            _latestSnapshot = new RaftSnapshot(meta, envelope);
            await _view.CompactThroughAsync(meta);
            Stats.CheckpointsWritten++;
            _s.Debug("raft", NodeServices.Inv($"{Name}: checkpoint at {index} (term {meta.Term}, {envelope.Length} bytes); log compacted."));
        }
        catch (Exception ex) when (!_closed && IsRecoverablePersistFailure(ex))
        {
            _s.Warn("raft", NodeServices.Inv($"{Name}: writing the checkpoint at {index} failed: {ex.Message}; retrying later."));
        }
    }

    /// <summary>Installs a snapshot received from the leader (verify, durable install, activate). False when it was rejected.</summary>
    private async Task<bool> InstallSnapshotAsync(RaftSnapshotMeta meta, ReadOnlyMemory<byte> data)
    {
        ulong appliedBefore = _appliedIndex;
        await _s.HitAsync(FaultStage.F16, FaultSites.RaftSnapshotInstall, VolumeName, DfsOperationKind.RaftSnapshot, detail: ResyncDetails.Verify);
        if (_closed)
            return false;
        if (!SnapshotEnvelope.TryDecode(data, meta, out _, out ReadOnlyMemory<byte> state, out string? error))
        {
            Stats.SnapshotVerificationFailures++;
            Stats.SnapshotInstallsAborted++;
            _s.Warn("raft", NodeServices.Inv($"{Name}: rejected snapshot ({meta.Index}, {meta.Term}): {error}"));
            await ResetFromDurableStateAsync("snapshot verification failure", reopenLog: false);
            return false;
        }

        await _s.HitAsync(FaultStage.F16, FaultSites.RaftSnapshotInstall, VolumeName, DfsOperationKind.RaftSnapshot, detail: ResyncDetails.Install);
        if (_closed)
            return false;
        try
        {
            await SaveCheckpointAsync(meta, data);
            await _view.CompactThroughAsync(meta);
        }
        catch (Exception)
        {
            Stats.SnapshotInstallsAborted++;
            throw;
        }

        await _s.HitAsync(FaultStage.F16, FaultSites.RaftSnapshotInstall, VolumeName, DfsOperationKind.RaftSnapshot, detail: ResyncDetails.Activate);
        if (_closed)
            return false;
        _sm.RestoreCheckpoint(new ReadOnlySequence<byte>(state), meta.Index, meta.Term);
        _appliedIndex = meta.Index;
        _appliedTerm = meta.Term;
        _appliedConf = meta.ConfState;
        _latestSnapshot = new RaftSnapshot(meta, data);
        Stats.SnapshotsInstalled++;
        Stats.EntriesCoveredByInstall += (long)(meta.Index - Math.Min(meta.Index, appliedBefore));
        _postInstallCatchUp = true;
        foreach (PendingProposal p in _proposals.Values.Where(p => p.Index <= meta.Index).ToArray())
        {
            _proposals.Remove(p.Key);
            Exception unknown = OutcomeUnknown(p.Context, $"a snapshot through {meta.Index} replaced the local log");
            Defer(() => p.Completion.TrySetException(unknown));
        }

        _s.Info("raft", NodeServices.Inv($"{Name}: installed snapshot ({meta.Index}, {meta.Term}) from the leader ({data.Length} bytes)."));
        _host.OnConfApplied(this, _appliedConf);
        _host.OnApplied(this, _appliedIndex);
        return true;
    }

    private async Task SaveCheckpointAsync(RaftSnapshotMeta meta, ReadOnlyMemory<byte> envelope)
    {
        await using CheckpointWriter writer = await _checkpoints.BeginWriteAsync(meta.Index, meta.Term, RaftConfCodec.Encode(meta.ConfState));
        for (int offset = 0; offset < envelope.Length; offset += CheckpointChunkBytes)
            await writer.WriteAsync(envelope.Slice(offset, Math.Min(CheckpointChunkBytes, envelope.Length - offset)));
        await writer.CompleteAsync();
    }

    private static async Task<RaftSnapshot?> LoadLatestCheckpointAsync(CheckpointStore checkpoints, GroupOpenSpec? spec, CancellationToken ct)
    {
        CheckpointReader? reader = await checkpoints.OpenLatestAsync(verifyContent: true, ct);
        if (reader is null)
            return null;
        await using (reader)
        {
            byte[] envelope = await reader.ReadAllAsync(ct);
            var meta = new RaftSnapshotMeta(reader.Index, reader.Term, RaftConfCodec.Decode(reader.Metadata.Span));
            if (!SnapshotEnvelope.TryDecode(envelope, meta, out _, out _, out string? error))
                throw new DfsIntegrityException($"Group '{spec?.Group ?? checkpoints.Directory}': the current checkpoint is unusable: {error}");
            return new RaftSnapshot(meta, envelope);
        }
    }
}
