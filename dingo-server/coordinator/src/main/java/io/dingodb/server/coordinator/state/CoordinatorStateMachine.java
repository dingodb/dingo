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

package io.dingodb.server.coordinator.state;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.netty.packet.message.EmptyMessage;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Iterator;
import io.dingodb.raft.Status;
import io.dingodb.raft.core.NodeImpl;
import io.dingodb.raft.core.StateMachineAdapter;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.error.RemotingException;
import io.dingodb.raft.rpc.RpcClient;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.rpc.impl.AbstractClientService;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import io.dingodb.raft.util.RecycleUtil;
import io.dingodb.server.coordinator.context.CoordinatorContext;
import io.dingodb.server.coordinator.handler.GetClusterInfoHandler;
import io.dingodb.server.coordinator.handler.GetLocationHandler;
import io.dingodb.server.coordinator.handler.GetLocationHandler.GetLocationRequest;
import io.dingodb.server.coordinator.handler.GetLocationHandler.GetLocationResponse;
import io.dingodb.server.coordinator.handler.GetStoreIdHandler;
import io.dingodb.server.coordinator.handler.GetStoreInfoHandler;
import io.dingodb.server.coordinator.handler.RegionHeartbeatHandler;
import io.dingodb.server.coordinator.handler.SetStoreHandler;
import io.dingodb.server.coordinator.handler.StoreHeartbeatHandler;
import io.dingodb.server.coordinator.service.StateService;
import io.dingodb.server.protocol.Tags;
import io.dingodb.server.protocol.code.BaseCode;
import io.dingodb.server.protocol.code.Code;
import io.dingodb.server.protocol.code.RaftServiceCode;
import io.dingodb.store.row.errors.IllegalRowStoreOperationException;
import io.dingodb.store.row.errors.StoreCodecException;
import io.dingodb.store.row.metrics.KVMetrics;
import io.dingodb.store.row.rpc.ExtSerializerSupports;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.storage.KVClosureAdapter;
import io.dingodb.store.row.storage.KVOperation;
import io.dingodb.store.row.storage.KVState;
import io.dingodb.store.row.storage.KVStateOutputList;
import io.dingodb.store.row.storage.RocksRawKVStore;
import io.dingodb.store.row.util.StackTraceUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.raft.rpc.RaftRpcServerFactory.createRaftRpcServer;
import static io.dingodb.server.protocol.ServerError.UNSUPPORTED_CODE;
import static io.dingodb.server.protocol.code.BaseCode.PONG;
import static io.dingodb.store.row.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static io.dingodb.store.row.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

@Slf4j
public class CoordinatorStateMachine extends StateMachineAdapter {

    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    private final Serializer serializer = Serializers.getDefault();

    private final CoordinatorContext context;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private StateService stateService;

    private Meter applyMeter;
    private Histogram batchWriteHistogram;
    private final String coordinatorRaftId;

    private final RocksRawKVStore rawKVStore;
    private CoordinatorStateSnapshot storeSnapshotFile;

    private final Set<Channel> leaderListener = new CopyOnWriteArraySet<>();

    private RpcClient rpcClient;

    public CoordinatorStateMachine(String coordinatorRaftId, RocksRawKVStore rawKVStore, CoordinatorContext context) {
        this.context = context;
        this.coordinatorRaftId = coordinatorRaftId;
        this.rawKVStore = rawKVStore;
    }

    public void init() {
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, coordinatorRaftId);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, coordinatorRaftId);
        this.storeSnapshotFile = new CoordinatorStateSnapshot(this.rawKVStore);
        initRpcServer();
        context.netService().registerMessageListenerProvider(Tags.RAFT_SERVICE, () -> this::onMessage);
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
                this.rawKVStore.batchPut(kvStates);
                break;
            case KVOperation.GET:
                this.rawKVStore.batchGet(kvStates);
                break;
            case KVOperation.DELETE:
                this.rawKVStore.batchDelete(kvStates);
                break;
            case KVOperation.CONTAINS_KEY:
                this.rawKVStore.batchContainsKey(kvStates);
                break;
            case KVOperation.GET_PUT:
                this.rawKVStore.batchGetAndPut(kvStates);
                break;
            case KVOperation.MERGE:
                this.rawKVStore.batchMerge(kvStates);
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
        context.scheduleMetaAdaptor().disable();
        context.tableMetaAdaptor().disable();
        context.rowStoreMetaAdaptor().disable();
        onStartSuccess();
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        stateService.stop();
        context.scheduleMetaAdaptor().disable();
        context.tableMetaAdaptor().disable();
        context.rowStoreMetaAdaptor().disable();
        onStopSuccess();
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        stateService = context.serviceProvider().leaderService(context);
        stateService.start();
        leaderListener.forEach(channel -> channel.send(SimpleMessage.EMPTY));
        context.scheduleMetaAdaptor().init();
        context.tableMetaAdaptor().init();
        context.rowStoreMetaAdaptor().init();
        context.metaService().init(context.tableMetaAdaptor());
        onStartSuccess();
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        this.leaderTerm.set(-1L);
        stateService.stop();
        context.scheduleMetaAdaptor().disable();
        context.tableMetaAdaptor().disable();
        context.rowStoreMetaAdaptor().disable();
        onStopSuccess();
    }

    public void onStartSuccess() {
        if (started.compareAndSet(false, true)) {
            rpcClient = ((AbstractClientService) ((NodeImpl) context.node()).getRpcService()).getRpcClient();
            try {
                context.netService().listenPort(context.configuration().port());
            } catch (Exception e) {
                log.error("Listen server port [{}] error.", context.configuration().instancePort(), e);
                throw new RuntimeException();
            }
        }
    }

    public void onStopSuccess() {
        try {
            context.netService().cancelPort(context.configuration().port());
        } catch (Exception e) {
            log.error("Cancel server port [{}] error.", context.configuration().instancePort(), e);
            throw new RuntimeException();
        }
    }

    private void onMessage(Message message, Channel channel) {
        ByteBuffer buffer = ByteBuffer.wrap(message.toBytes());
        Code code = Code.valueOf(PrimitiveCodec.readZigZagInt(buffer));
        if (code instanceof RaftServiceCode) {
            switch ((RaftServiceCode) code) {
                case GET_LEADER_LOCATION:
                    getLeaderLocation(channel);
                    break;
                case GET_ALL_LOCATION:
                    getAllLocation(channel);
                    break;
                case LISTEN_LEADER:
                    log.info("New leader listener channel, remote: [{}]", channel.remoteAddress());
                    leaderListener.add(channel);
                    channel.closeListener(leaderListener::remove);
                    break;
                default:
                    channel.send(UNSUPPORTED_CODE.message());
                    break;
            }
        } else if (code instanceof BaseCode) {
            switch ((BaseCode) code) {
                case PING:
                    channel.registerMessageListener(this::onMessage);
                    channel.send(PONG.message());
                    break;
                case OTHER:
                    break;
                default:
                    channel.send(UNSUPPORTED_CODE.message());
                    break;
            }
        }
    }

    private void getAllLocation(Channel channel) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            List<PeerId> peerIds = context.node().listPeers();
            baos.write(PrimitiveCodec.encodeVarInt(peerIds.size()));
            for (PeerId peerId : peerIds) {
                GetLocationResponse res = (GetLocationResponse) rpcClient
                    .invokeSync(peerId.getEndpoint(), GetLocationRequest.INSTANCE, 3000);
                baos.write(encodeHostPort(res.getHost(), res.getPort()));
            }
            baos.flush();
            channel.send(new SimpleMessage(null, baos.toByteArray()));
        } catch (IOException e) {
            log.error("Serialize leader location error", e);
            channel.send(EmptyMessage.INSTANCE);
        } catch (RemotingException e) {
            log.error("Get peer location error", e);
            channel.send(EmptyMessage.INSTANCE);
        } catch (InterruptedException e) {
            log.error("Get all location interrupt.", e);
            channel.send(EmptyMessage.INSTANCE);
        } catch (Exception e) {
            log.error("Get leader location error.", e);
            channel.send(EmptyMessage.INSTANCE);
        }
    }

    private void getLeaderLocation(Channel channel) {
        try {
            GetLocationResponse res = (GetLocationResponse) rpcClient.invokeSync(
                context.node().getLeaderId().getEndpoint(),
                GetLocationRequest.INSTANCE,
                3000
            );
            channel.send(SimpleMessage.builder().content(encodeHostPort(res.getHost(), res.getPort())).build());
        } catch (IOException e) {
            log.error("Serialize location error", e);
            channel.send(EmptyMessage.INSTANCE);
        } catch (RemotingException e) {
            log.error("Get leader peer location error", e);
            channel.send(EmptyMessage.INSTANCE);
        } catch (InterruptedException e) {
            log.error("Get leader location interrupt.", e);
            channel.send(EmptyMessage.INSTANCE);
        } catch (Exception e) {
            log.error("Get leader location error.", e);
            channel.send(EmptyMessage.INSTANCE);
        }
    }

    private byte[] encodeHostPort(String host, int port) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            baos.write(PrimitiveCodec.encodeString(host));
            baos.write(PrimitiveCodec.encodeVarInt(port));
            baos.flush();
            return baos.toByteArray();
        }
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    private RpcServer initRpcServer() {
        ExtSerializerSupports.init();
        RpcServer rpcServer = createRaftRpcServer(context.endpoint(), raftExecutor(), cliExecutor());
        rpcServer.registerProcessor(new GetLocationHandler());
        rpcServer.registerProcessor(new GetClusterInfoHandler(context.rowStoreMetaAdaptor()));
        rpcServer.registerProcessor(new GetStoreInfoHandler(context.rowStoreMetaAdaptor()));
        rpcServer.registerProcessor(new GetStoreIdHandler(context.rowStoreMetaAdaptor()));
        rpcServer.registerProcessor(new RegionHeartbeatHandler(context.rowStoreMetaAdaptor()));
        rpcServer.registerProcessor(new SetStoreHandler(context.rowStoreMetaAdaptor()));
        rpcServer.registerProcessor(new StoreHeartbeatHandler(context.rowStoreMetaAdaptor()));
        log.info("Start coordinator raft rpc server, result: {}.", rpcServer.init(null));
        context.rpcServer(rpcServer);
        return rpcServer;
    }

    private ThreadPoolExecutor raftExecutor() {
        return new ThreadPoolBuilder()
            .name("Raft-leader")
            .build();
    }

    private ThreadPoolExecutor cliExecutor() {
        return new ThreadPoolBuilder()
            .name("Cli-leader")
            .build();
    }
}
