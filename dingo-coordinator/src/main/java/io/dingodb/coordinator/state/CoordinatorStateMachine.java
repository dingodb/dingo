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

package io.dingodb.coordinator.state;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import io.dingodb.coordinator.context.CoordinatorContext;
import io.dingodb.coordinator.service.StateService;
import io.dingodb.store.row.errors.IllegalRowStoreOperationException;
import io.dingodb.store.row.errors.StoreCodecException;
import io.dingodb.store.row.metrics.KVMetrics;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.storage.KVClosureAdapter;
import io.dingodb.store.row.storage.KVOperation;
import io.dingodb.store.row.storage.KVState;
import io.dingodb.store.row.storage.KVStateOutputList;
import io.dingodb.store.row.storage.RocksRawKVStore;
import io.dingodb.store.row.util.StackTraceUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.store.row.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static io.dingodb.store.row.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

@Slf4j
public class CoordinatorStateMachine extends StateMachineAdapter {

    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    private final Serializer serializer = Serializers.getDefault();

    private final CoordinatorContext context;
    private StateService stateService;

    private final Meter applyMeter;
    private final Histogram batchWriteHistogram;
    private final String coordinatorRaftId;

    private final RocksRawKVStore rocksRawKVStore;
    private final CoordinatorStateSnapshot storeSnapshotFile;

    public CoordinatorStateMachine(CoordinatorContext context) {
        this.context = context;
        this.coordinatorRaftId = context.configuration().coordinatorRaftId();
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, coordinatorRaftId);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, coordinatorRaftId);
        this.rocksRawKVStore = context.rocksKVStore();
        this.storeSnapshotFile = new CoordinatorStateSnapshot(this.rocksRawKVStore);
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
            log.error("StateMachine meet critical error: {}.", StackTraceUtil.stackTrace(t));
            it.setErrorAndRollback(
                index - applied,
                new Status(RaftError.ESTATEMACHINE, "StateMachine meet critical error: %s.", t.getMessage())
            );
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
            recordMetrics(opByte, size);
            batchApply(opByte, kvStates);
            return size;
        } finally {
            RecycleUtil.recycle(kvStates);
        }
    }

    private void recordMetrics(byte opByte, int size) {
        KVMetrics.meter(
            STATE_MACHINE_APPLY_QPS,
            coordinatorRaftId,
            KVOperation.opName(opByte)
        ).mark(size);
        this.batchWriteHistogram.update(size);
    }


    private void batchApply(final byte opType, final KVStateOutputList kvStates) {
        switch (opType) {
            case KVOperation.PUT:
                this.rocksRawKVStore.batchPut(kvStates);
                break;
            case KVOperation.GET:
                this.rocksRawKVStore.batchGet(kvStates);
                break;
            case KVOperation.DELETE:
                this.rocksRawKVStore.batchDelete(kvStates);
                break;
            case KVOperation.CONTAINS_KEY:
                this.rocksRawKVStore.batchContainsKey(kvStates);
                break;
            case KVOperation.GET_PUT:
                this.rocksRawKVStore.batchGetAndPut(kvStates);
                break;
            case KVOperation.MERGE:
                this.rocksRawKVStore.batchMerge(kvStates);
                break;
            default:
                throw new IllegalRowStoreOperationException("Unknown operation: " + opType);
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        this.storeSnapshotFile.save(writer, done);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        return this.storeSnapshotFile.load(reader);
    }


    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        if (stateService != null) {
            stateService.stop();
        }
        stateService = context.serviceProvider().followerService(context);
        stateService.start();
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        stateService.stop();
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        stateService = context.serviceProvider().leaderService(context);
        stateService.start();
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        this.leaderTerm.set(-1L);
        stateService.stop();
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }



}
