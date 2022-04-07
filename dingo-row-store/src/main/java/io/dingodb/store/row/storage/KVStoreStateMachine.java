/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.store.row.storage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Iterator;
import io.dingodb.raft.Status;
import io.dingodb.raft.core.StateMachineAdapter;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.ReportTarget;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import io.dingodb.raft.util.BytesUtil;
import io.dingodb.raft.util.RecycleUtil;
import io.dingodb.raft.util.internal.ThrowUtil;
import io.dingodb.store.row.StateListener;
import io.dingodb.store.row.StoreEngine;
import io.dingodb.store.row.client.StoreRpcClient;
import io.dingodb.store.row.errors.Errors;
import io.dingodb.store.row.errors.IllegalRowStoreOperationException;
import io.dingodb.store.row.errors.StoreCodecException;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metrics.KVMetrics;
import io.dingodb.store.row.rpc.ApiStatus;
import io.dingodb.store.row.rpc.ReportToLeaderApi;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.util.StackTraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.store.row.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static io.dingodb.store.row.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class KVStoreStateMachine extends StateMachineAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(KVStoreStateMachine.class);

    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    private final Serializer serializer = Serializers.getDefault();
    private final Region region;
    private final StoreEngine storeEngine;
    private final BatchRawKVStore<?> rawKVStore;
    private final KVStoreSnapshotFile storeSnapshotFile;
    private final Meter applyMeter;
    private final Histogram batchWriteHistogram;

    public KVStoreStateMachine(Region region, StoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.rawKVStore = storeEngine.getRawKVStore();
        this.storeSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.rawKVStore);
        final String regionStr = String.valueOf(this.region.getId());
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, regionStr);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, regionStr);
    }

    @Override
    public void onApply(final Iterator it) {
        int index = 0;
        int applied = 0;
        try {
            KVStateOutputList kvStates = KVStateOutputList.newInstance();
            while (it.hasNext()) {
                KVOperation kvOp;
                final KVClosureAdapter done = (KVClosureAdapter) it.done();
                if (done != null) {
                    kvOp = done.getOperation();
                } else {
                    final ByteBuffer buf = it.getData();
                    try {
                        if (buf.hasArray()) {
                            kvOp = this.serializer.readObject(buf.array(), KVOperation.class);
                        } else {
                            kvOp = this.serializer.readObject(buf, KVOperation.class);
                        }
                    } catch (final Throwable t) {
                        ++index;
                        throw new StoreCodecException("Decode operation error", t);
                    }
                }
                final KVState first = kvStates.getFirstElement();
                if (first != null && !first.isSameOp(kvOp)) {
                    applied += batchApplyAndRecycle(first.getOpByte(), kvStates);
                    kvStates = KVStateOutputList.newInstance();
                }
                kvStates.add(KVState.of(kvOp, done));
                ++index;
                it.next();
            }
            if (!kvStates.isEmpty()) {
                final KVState first = kvStates.getFirstElement();
                assert first != null;
                applied += batchApplyAndRecycle(first.getOpByte(), kvStates);
            }
        } catch (final Throwable t) {
            LOG.error("StateMachine meet critical error: {}.", StackTraceUtil.stackTrace(t));
            it.setErrorAndRollback(index - applied, new Status(RaftError.ESTATEMACHINE,
                "StateMachine meet critical error: %s.", t.getMessage()));
        } finally {
            // metrics: qps
            this.applyMeter.mark(applied);
        }
    }

    private int batchApplyAndRecycle(final byte opByte, final KVStateOutputList kvStates) {
        try {
            final int size = kvStates.size();

            if (size == 0) {
                return 0;
            }

            if (!KVOperation.isValidOp(opByte)) {
                throw new IllegalRowStoreOperationException("Unknown operation: " + opByte);
            }

            // metrics: op qps
            final Meter opApplyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, String.valueOf(this.region.getId()),
                KVOperation.opName(opByte));
            opApplyMeter.mark(size);
            this.batchWriteHistogram.update(size);

            // do batch apply
            batchApply(opByte, kvStates);

            return size;
        } finally {
            RecycleUtil.recycle(kvStates);
        }
    }

    private void batchApply(final byte opType, final KVStateOutputList kvStates) {
        switch (opType) {
            case KVOperation.PUT:
                this.rawKVStore.batchPut(kvStates);
                break;
            case KVOperation.PUT_IF_ABSENT:
                this.rawKVStore.batchPutIfAbsent(kvStates);
                break;
            case KVOperation.PUT_LIST:
                this.rawKVStore.batchPutList(kvStates);
                break;
            case KVOperation.DELETE:
                this.rawKVStore.batchDelete(kvStates);
                break;
            case KVOperation.DELETE_RANGE:
                this.rawKVStore.batchDeleteRange(kvStates);
                break;
            case KVOperation.DELETE_LIST:
                this.rawKVStore.batchDeleteList(kvStates);
                break;
            case KVOperation.GET_SEQUENCE:
                this.rawKVStore.batchGetSequence(kvStates);
                break;
            case KVOperation.NODE_EXECUTE:
                this.rawKVStore.batchNodeExecute(kvStates, isLeader());
                break;
            case KVOperation.KEY_LOCK:
                this.rawKVStore.batchTryLockWith(kvStates);
                break;
            case KVOperation.KEY_LOCK_RELEASE:
                this.rawKVStore.batchReleaseLockWith(kvStates);
                break;
            case KVOperation.GET:
                this.rawKVStore.batchGet(kvStates);
                break;
            case KVOperation.MULTI_GET:
                this.rawKVStore.batchMultiGet(kvStates);
                break;
            case KVOperation.CONTAINS_KEY:
                this.rawKVStore.batchContainsKey(kvStates);
                break;
            case KVOperation.SCAN:
                this.rawKVStore.batchScan(kvStates);
                break;
            case KVOperation.REVERSE_SCAN:
                this.rawKVStore.batchReverseScan(kvStates);
                break;
            case KVOperation.GET_PUT:
                this.rawKVStore.batchGetAndPut(kvStates);
                break;
            case KVOperation.COMPARE_PUT:
                this.rawKVStore.batchCompareAndPut(kvStates);
                break;
            case KVOperation.COMPARE_PUT_ALL:
                this.rawKVStore.batchCompareAndPutAll(kvStates);
                break;
            case KVOperation.MERGE:
                this.rawKVStore.batchMerge(kvStates);
                break;
            case KVOperation.RESET_SEQUENCE:
                this.rawKVStore.batchResetSequence(kvStates);
                break;
            case KVOperation.RANGE_SPLIT:
                doSplit(kvStates);
                break;
            default:
                throw new IllegalRowStoreOperationException("Unknown operation: " + opType);
        }
    }

    private void doSplit(final KVStateOutputList kvStates) {
        final byte[] parentKey = this.region.getStartKey();
        for (final KVState kvState : kvStates) {
            final KVOperation op = kvState.getOp();
            final String currentRegionId = op.getCurrentRegionId();
            final String newRegionId = op.getNewRegionId();
            final byte[] splitKey = op.getKey();
            final KVStoreClosure closure = kvState.getDone();
            try {
                this.rawKVStore.initFencingToken(parentKey, splitKey);
                this.storeEngine.doSplit(currentRegionId, newRegionId, splitKey);
                if (closure != null) {
                    // null on follower
                    closure.setData(Boolean.TRUE);
                    closure.run(Status.OK());
                }
            } catch (final Throwable t) {
                LOG.error("Fail to split, regionId={}, newRegionId={}, splitKey={}.", currentRegionId, newRegionId,
                    BytesUtil.toHex(splitKey));
                setCriticalError(closure, t);
            }
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done, ReportTarget reportTarget) {
        if (reportTarget != null) {
            reportTarget.setRegionId(this.getRegionId());
        }
        this.storeSnapshotFile.save(writer, this.region.copy(), done, this.storeEngine.getSnapshotExecutor(), reportTarget);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        return this.storeSnapshotFile.load(reader, this.region.copy());
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
            .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onLeaderStart(term);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        final long oldTerm = this.leaderTerm.get();
        this.leaderTerm.set(-1L);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we asynchronously
        // triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
            .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onLeaderStop(oldTerm);
            }
        });
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
            .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onStartFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    @Override
    public void onReportFreezeSnapshotResult(final boolean freezeResult, final String errMsg,
                                             ReportTarget reportTarget) {
        LOG.info("StateMachineAdapter onReportFreezeSnapshotResult, freezeResult: {}, errMsg: {}" +
            ", reportTarget: {}.", freezeResult, errMsg, reportTarget);
        reportTarget.setRegionId(this.getRegionId());
        StoreRpcClient client = new StoreRpcClient();
        ReportToLeaderApi api = client.getReportToLeadeApi(reportTarget.getIp(), reportTarget.getPort());
        ApiStatus status = api.freezeSnapshotResult(reportTarget.getRegionId(), freezeResult, errMsg);
        if (status != ApiStatus.OK) {
            LOG.error("KVStoreStateMachine onReportFreezeSnapshotResult, regionId: {}, leaderIp: {}, " +
                    "exchangePort: {}, api status: {}.", reportTarget.getRegionId(), reportTarget.getIp(),
                reportTarget.getPort(), status.name());
        }
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
            .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onStopFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public String getRegionId() {
        return this.region.getId();
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closure callback
     * @param ex      critical error
     */
    private static void setCriticalError(final KVStoreClosure closure, final Throwable ex) {
        // Will call closure#run in FSMCaller
        if (closure != null) {
            closure.setError(Errors.forException(ex));
        }
        ThrowUtil.throwException(ex);
    }
}
