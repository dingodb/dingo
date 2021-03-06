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

package io.dingodb.raft.kv.storage;

import com.google.protobuf.ByteString;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Iterator;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.Status;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.entity.LocalFileMetaOutter;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.error.RaftException;
import io.dingodb.raft.rpc.ReportTarget;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import static io.dingodb.raft.kv.Constants.SNAPSHOT_ZIP;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.SNAPSHOT_LOAD;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.SNAPSHOT_SAVE;

@Slf4j
public class DefaultRaftRawKVStoreStateMachine implements StateMachine {

    protected final AtomicLong leaderTerm = new AtomicLong(-1L);
    protected final String id;
    protected final RaftRawKVStore store;

    public DefaultRaftRawKVStoreStateMachine(String id, RaftRawKVStore store) {
        this.store = store;
        this.id = id;
    }

    @Override
    public void onApply(final Iterator it) {
        int applied = 0;
        try {
            RaftRawKVOperation operation = null;
            while (it.hasNext()) {
                try {
                    operation = RaftRawKVOperation.decode(it.getData());
                    apply(operation, it.done());
                    applied++;
                } catch (Exception e) {
                    if (it.done() instanceof RaftClosure) {
                        it.done().run(new Status(-1, "Apply %s operation error: %s.", operation, e.getMessage()));
                    }
                    throw e;
                }
                it.next();
            }
        } catch (final Throwable t) {
            log.error("StateMachine meet critical error: {}.", t.getMessage(), t);
            it.setErrorAndRollback(it.getIndex() - applied, new Status(RaftError.ESTATEMACHINE,
                "StateMachine meet critical error: %s.", t.getMessage()));
        } finally {
            this.applyMeter(applied);
        }
    }

    protected void onApplyOperation(RaftRawKVOperation operation) {
    }

    private void apply(RaftRawKVOperation operation, Closure closure) {
        if (log.isDebugEnabled()) {
            log.debug("Apply operation: {}", operation);
        }
        if (closure instanceof  RaftClosure) {
            ((RaftClosure<?>) closure).complete(store.executeLocal(operation));
        } else {
            store.executeLocal(operation);
        }
        Executors.submit(id + " on apply", () -> onApplyOperation(operation));
    }

    private void applyMeter(int applied) {

    }

    @Override
    public void onShutdown() {
        log.info("onShutdown.");
    }

    public RaftRawKVOperation snapshotSaveOperation(final SnapshotWriter writer, final Closure done) {
        return RaftRawKVOperation.builder()
            .ext1(writer.getPath())
            .op(SNAPSHOT_SAVE)
            .build();
    }

    public RaftRawKVOperation snapshotLoadOperation(final SnapshotReader reader) {
        return RaftRawKVOperation.builder()
            .ext1(reader.getPath())
            .ext2(((LocalFileMetaOutter.LocalFileMeta) reader.getFileMeta(SNAPSHOT_ZIP)).getChecksum())
            .op(SNAPSHOT_LOAD)
            .build();
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        store.snapshotSave(snapshotSaveOperation(writer, done)).whenCompleteAsync((checksum, ex) -> {
            LocalFileMetaOutter.LocalFileMeta.Builder metaBuilder = LocalFileMetaOutter.LocalFileMeta.newBuilder();
            metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));
            metaBuilder.setUserMeta(ByteString.copyFromUtf8(store.getRaftId().toString()));
            writer.addFile(SNAPSHOT_ZIP, metaBuilder.build());
            done.run(Status.OK());
        }, Executors.executor("snapshot-save-" + id));
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done, ReportTarget reportTarget) {
        onSnapshotSave(writer, done);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        return store
            .snapshotLoad(snapshotLoadOperation(reader))
            .join();
    }

    @Override
    public void onReportFreezeSnapshotResult(boolean freezeResult, String errMsg, ReportTarget reportTarget) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRegionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onLeaderStart(final long term) {
        log.info("onLeaderStart: term={}.", term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        log.info("onLeaderStop: status={}.", status);
    }

    @Override
    public void onError(final RaftException ex) {
        log.error(
            "Encountered {} on {}, you should figure out the cause and repair or remove this node.",
            ex.getStatus(), getClass().getName(), ex
        );
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        log.info("onConfigurationCommitted: {}.", conf);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        log.info("onStopFollowing: {}.", ctx);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        log.info("onStartFollowing: {}.", ctx);
    }
}
