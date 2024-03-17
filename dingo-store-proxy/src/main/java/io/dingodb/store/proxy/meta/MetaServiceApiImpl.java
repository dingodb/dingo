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

package io.dingodb.store.proxy.meta;

import io.dingodb.cluster.ClusterService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.service.ListenService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockService;
import io.dingodb.tso.TsoService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static io.dingodb.common.CommonId.CommonType.CLUSTER;
import static io.dingodb.common.CommonId.CommonType.SDK;
import static io.dingodb.sdk.service.Services.metaService;
import static io.dingodb.transaction.api.LockType.RANGE;
import static io.dingodb.transaction.api.LockType.TABLE;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class MetaServiceApiImpl implements MetaServiceApi {
    private static final ApiRegistry apis = ApiRegistry.getDefault();

    private static final long participantJoin = 0;

    private static final CommonId participantJoinCommonId = new CommonId(CLUSTER, 0, participantJoin);

    private static final Consumer<Message> participantJoinListener = ListenService.getDefault()
        .register(participantJoinCommonId, null);

    private static final CommonId ID = DingoConfiguration.serverId();

    public static final MetaServiceApiImpl INSTANCE = new MetaServiceApiImpl();

    private final io.dingodb.sdk.service.MetaService proxyService = metaService(Configuration.coordinatorSet());
    private final io.dingodb.sdk.service.LockService lockService = new io.dingodb.sdk.service.LockService(
        "MetaNode", Configuration.coordinators()
    );

    private final VersionService versionService = Services.versionService(Configuration.coordinatorSet());

    private io.dingodb.sdk.service.LockService.Lock lock;

    private CommonId leaderId;
    private Channel leaderChannel;
    private final Map<CommonId, Location> participantLocations = new ConcurrentHashMap<>();
    private final Map<CommonId, Channel> participantChannels = new ConcurrentHashMap<>();

    private final Map<String, TableLock> tableLocks = new ConcurrentHashMap<>();

    private boolean needLock = true;
    private final LinkedRunner lockRunner = new LinkedRunner("lock-runner");

    private MetaServiceApiImpl() {
        lock = lockService.newLock(ID + "#" + DingoConfiguration.location().url());
        lock.watchDestroy().thenRunAsync(() -> {
            lock = lockService.newLock(ID + "#" + DingoConfiguration.location().url());
            lock();
        }, Executors.executor("meta-lock"));
        apis.register(MetaServiceApi.class, this);
        Executors.execute("meta-lock", this::lock, true);
    }

    private void retryLock() {
        needLock = true;
        lockRunner.forceFollow(this::lock);
    }

    private void lock() {
        if (!needLock) {
            return;
        }
        log.info("Meta lock start...");
        leaderId = null;
        leaderChannel = null;
        try {
            if (ID.type == SDK || !lock.tryLock()) {
                Kv currentLock = lockService.currentLock();
                String[] ss = new String(currentLock.getKv().getValue()).split("#");
                CommonId leaderId = CommonId.parse(ss[0]);
                Location leaderLocation = Location.parseUrl(ss[1]);
                if (ID.equals(leaderId) || leaderLocation.equals(DingoConfiguration.location())) {
                    log.info(
                        "Old leader location equals current location, but id not equals, old id: {}, current id: {}.",
                        ID, leaderId
                    );
                    lockService.delete(ID.seq, new String(currentLock.getKv().getKey()));
                    retryLock();
                    return;
                }
                leaderChannel = NetService.getDefault().newChannel(leaderLocation);
                this.leaderChannel.setCloseListener(ch -> this.retryLock());
                proxy(this.leaderChannel).connect(null, ID, DingoConfiguration.location());
                scanTableLock();
                lockService.watchLock(currentLock, this::retryLock);
                this.leaderId = leaderId;
                log.info("Current {}, leader: {}.", ID, leaderId);
                needLock = false;
            } else {
                scanTableLock();
                log.info("Become leader, id {}.", ID);
                lock.watchDestroy().thenRun(this::retryLock);
            }
        } catch (Exception e) {
            if (leaderChannel != null) {
                leaderChannel.close();
            }
            log.error("Meta lock error, will retry.", e);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            retryLock();
        }
    }

    public boolean isLeader() {
        return lock.getLocked() > 0 && !lock.watchDestroy().isDone();
    }

    public CommonId leader() {
        return leaderId;
    }

    public Channel leaderChannel() {
        if (leaderChannel == null || leaderChannel.isClosed()) {
            retryLock();
            throw new RuntimeException("Offline, please wait and retry.");
        }
        return leaderChannel.cloneChannel();
    }

    public boolean isReady() {
        return isLeader() || leaderId != null;
    }

    @Override
    public synchronized void connect(Channel channel, CommonId serverId, Location location) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        Optional.ofNullable(participantChannels.remove(serverId))
            .ifPresent(ch -> ch.setCloseListener(null))
            .ifPresent(Channel::close);
        participantChannels.put(serverId, channel);
        participantLocations.put(serverId, location);
        log.info("Participant {} join.", serverId);
        channel.setCloseListener(ch -> {
            participantChannels.remove(serverId);
            log.info("Participant {} leave.", serverId);
        });
        participantJoinListener.accept(new Message(serverId.encode()));
    }

    @Override
    public Location getLocation(CommonId serverId) {
        Location location = participantLocations.get(serverId);
        if (location == null) {
            location = ClusterService.getDefault().getLocation(serverId);
        }
        return location;
    }

    private MetaServiceApi proxy(Location location) {
        return apis.proxy(MetaServiceApi.class, location);
    }

    private MetaServiceApi proxy(Channel channel) {
        return apis.proxy(MetaServiceApi.class, channel);
    }

    private void scanTableLock() {
        byte[] key = "Lock".getBytes(UTF_8);
        byte[] end = Arrays.copyOf(key, key.length);
        end[end.length - 1] = ++end[end.length - 1];
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(key)
            .rangeEnd(end)
            .build();
        long tso = TsoService.getDefault().tso();
        RangeResponse rangeResponse = versionService.kvRange(tso, rangeRequest);
        do {
            List<Kv> lockKvs = rangeResponse.getKvs();
            if (lockKvs != null && !lockKvs.isEmpty()) {
                for (Kv lockKv : lockKvs) {
                    String[] lockSs = new String(lockKv.getKv().getKey(), UTF_8).split("\\|");
                    TableLock tableLock = TableLock.builder()
                        .tableId(CommonId.parse(lockSs[1]))
                        .serverId(CommonId.parse(lockSs[2]))
                        .lockTs(Long.parseLong(lockSs[3]))
                        .currentTs(Long.parseLong(lockSs[4]))
                        .type(TABLE)
                        .build();
                    syncTableLock(tableLock);
                }
            }
        } while (rangeResponse.isMore());
    }

    private void pubTableLock(TableLock lock) {
        String key = "Lock" + "|" + lock.tableId + "|" + lock.serverId + "|" + lock.lockTs + "|" + lock.currentTs;
        if (log.isDebugEnabled()) {
            log.debug("Pub table lock [{}].", key);
        }
        lockService.put(lock.lockTs, key, null);
        if (log.isDebugEnabled()) {
            log.debug("Pub table lock [{}] success, add lock watch.", key);
        }
        lock.unlockFuture.thenRunAsync(() -> {
            lockService.delete(lock.lockTs, key);
            if (log.isDebugEnabled()) {
                log.debug("Unlock table lock [{}] success.", key);
            }
        });
        lockService.watchLock(
            Kv.builder().kv(KeyValue.builder().key(key.getBytes(UTF_8)).build()).build(),
            () -> {
                if (!lock.unlockFuture.isDone()) {
                    log.warn("Lose table lock {}.", key);
                    lock.unlockFuture.completeExceptionally(new UnknownError("Lose table lock key " + key));
                }
            }
        );
    }

    private synchronized boolean subTableLock(TableLock lock) {
        String key = "Lock" + "|" + lock.tableId + "|" + lock.serverId + "|" + lock.lockTs + "|" + lock.currentTs;
        if (log.isDebugEnabled()) {
            log.debug("Sub table lock [{}] success, add lock watch.", key);
        }
        if (tableLocks.containsKey(key)) {
            if (log.isDebugEnabled()) {
                log.debug("Table lock {} exist, skip subscribe.", key);
            }
            return false;
        }
        lockService.watchLock(
            Kv.builder().kv(KeyValue.builder().key(key.getBytes(UTF_8)).build()).build(),
            () -> lock.unlockFuture.complete(null)
        );
        tableLocks.put(key, lock);
        lock.unlockFuture.thenRun(() -> {
            tableLocks.remove(key);
            if (log.isDebugEnabled()) {
                log.debug("Table lock {} unlock.", key);
            }
        });
        return true;
    }

    @Override
    @SneakyThrows
    public void lockTable(long requestId, TableLock lock) {
        if (lock.serverId.equals(ID)) {
            pubTableLock(lock);
        }
        if (isLeader()) {
            broadcastTableLock(requestId, lock);
        } else if (lock.serverId.equals(ID)) {
            try (Channel channel = leaderChannel()) {
                MetaServiceApi metaServiceApi = proxy(channel);
                metaServiceApi.syncTableLock(lock);
            }
        }
    }

    @Override
    @SneakyThrows
    public void syncTableLock(TableLock lock) {
        if (lock.type != TABLE && lock.type != RANGE) {
            throw new RuntimeException("Supported only table and range.");
        }
        if (lock.serverId.equals(ID)) {
            log.warn("Remote lock request, but server equals current server id, lock: {}.", lock);
            return;
        }
        CompletableFuture<Boolean> lockFuture = new CompletableFuture<>();
        CompletableFuture<Void> unlockFuture = new CompletableFuture<>();
        lock = TableLock.builder()
            .serverId(lock.serverId)
            .lockTs(lock.lockTs)
            .currentTs(lock.currentTs)
            .tableId(lock.tableId)
            .type(lock.type)
            .lockFuture(lockFuture)
            .unlockFuture(unlockFuture)
            .build();
        try {
            if (!subTableLock(lock)) {
                return;
            }
            io.dingodb.store.proxy.service.TableLockService.INSTANCE.doLock(lock);
            lockFuture.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            lockFuture.cancel(true);
            unlockFuture.complete(null);
            throw e;
        }
    }

    @SneakyThrows
    private void broadcastTableLock(long requestId, TableLock lock) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        for (Map.Entry<CommonId, Channel> entry : participantChannels.entrySet()) {
            if (lock.serverId.equals(entry.getKey())) {
                continue;
            }
            try (Channel channel = entry.getValue().cloneChannel()) {
                proxy(channel).syncTableLock(lock);
            }
        }
    }

    @Override
    @SneakyThrows
    public void createTables(long requestId, String schema, String table, CreateTablesRequest request) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (isLeader()) {
            proxyService.createTables(requestId, request);
        } else {
            try (Channel channel = leaderChannel()) {
                proxy(channel).createTables(requestId, schema, table, request);
            }
        }
    }

    @Override
    @SneakyThrows
    public void dropTables(long requestId, String schema, String table, DropTablesRequest request) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (isLeader()) {
            proxyService.dropTables(requestId, request);
        } else {
            try (Channel channel = leaderChannel()) {
                proxy(channel).dropTables(requestId, schema, table, request);
            }
        }
    }

    @Override
    @SneakyThrows
    public void createSchema(long requestId, String schema, CreateSchemaRequest request) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (isLeader()) {
            proxyService.createSchema(requestId, request);
        } else {
            try (Channel channel = leaderChannel()) {
                proxy(channel).createSchema(requestId, schema, request);
            }
        }
    }

    @Override
    @SneakyThrows
    public void dropSchema(long requestId, String schema, DropSchemaRequest request) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (isLeader()) {
            proxyService.dropSchema(requestId, request);
        } else {
            try (Channel channel = leaderChannel()) {
                proxy(channel).dropSchema(requestId, schema, request);
            }
        }
    }
}
