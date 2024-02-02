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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.service.ListenService;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static io.dingodb.common.CommonId.CommonType.CLUSTER;
import static io.dingodb.common.CommonId.CommonType.SCHEMA_NOTIFY;
import static io.dingodb.common.CommonId.CommonType.TABLE_NOTIFY;
import static io.dingodb.sdk.service.Services.metaService;
import static io.dingodb.transaction.api.LockType.RANGE;
import static io.dingodb.transaction.api.LockType.TABLE;

@Slf4j
public class MetaServiceApiImpl implements MetaServiceApi {
    private static final ApiRegistry apis = ApiRegistry.getDefault();

    private static final long participantJoin = 0;
    private static final long createTableSeq = 0;
    private static final long dropTableSeq = 1;
    private static final long createSchemaSeq = 0;
    private static final long dropSchemaSeq = 1;

    private static final CommonId participantJoinCommonId = new CommonId(CLUSTER, 0, participantJoin);
    private static final CommonId createTableCommonId = new CommonId(TABLE_NOTIFY, 0, createTableSeq);
    private static final CommonId dropTableCommonId = new CommonId(TABLE_NOTIFY, 0, dropTableSeq);
    private static final CommonId createSchemaCommonId = new CommonId(SCHEMA_NOTIFY, 0, createSchemaSeq);
    private static final CommonId dropSchemaCommonId = new CommonId(SCHEMA_NOTIFY, 0, dropSchemaSeq);

    private static final Consumer<Message> participantJoinListener = ListenService.getDefault()
        .register(participantJoinCommonId, null);
    private static final Consumer<Message> createTableListener = ListenService.getDefault()
        .register(createTableCommonId, null);
    private static final Consumer<Message> dropTableListener = ListenService.getDefault()
        .register(dropTableCommonId, null);
    private static final Consumer<Message> createSchemaListener = ListenService.getDefault()
        .register(createSchemaCommonId, null);
    private static final Consumer<Message> dropSchemaListener = ListenService.getDefault()
        .register(dropSchemaCommonId, null);

    private static final CommonId ID = DingoConfiguration.serverId();

    public static final MetaServiceApiImpl INSTANCE = new MetaServiceApiImpl();

    private final io.dingodb.sdk.service.MetaService proxyService = metaService(Configuration.coordinatorSet());
    private final io.dingodb.sdk.service.LockService lockService = new io.dingodb.sdk.service.LockService("MetaNode", Configuration.coordinators());
    private final VersionService versionService = Services.versionService(Configuration.coordinatorSet());

    private io.dingodb.sdk.service.LockService.Lock lock;

    private CommonId leaderId;
    private Channel leaderChannel;
    private final Map<CommonId, Location> participantLocations = new ConcurrentHashMap<>();
    private final Map<CommonId, Channel> participantChannels = new ConcurrentHashMap<>();

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
        MetaService.ROOT.cache.clear();
        try {
            if (!lock.tryLock()) {
                Kv currentLock = lockService.currentLock();
                String[] ss = new String(currentLock.getKv().getValue()).split("#");
                CommonId leaderId = CommonId.parse(ss[0]);
                Location leaderLocation = Location.parseUrl(ss[1]);
                if (!ID.equals(leaderId) && leaderLocation.equals(DingoConfiguration.location())) {
                    log.info(
                        "Old leader location equals current location, but id not equals, old id: {}, current id: {}.",
                        ID, leaderId
                    );
                    lockService.delete(ID.seq, new String(currentLock.getKv().getKey()));
                    retryLock();
                    return;
                }
                Channel leaderChannel = NetService.getDefault().newChannel(leaderLocation);
                try {
                    this.leaderChannel = leaderChannel.cloneChannel();
                    this.leaderChannel.setCloseListener(ch -> this.retryLock());
                    participantChannels.put(leaderId, leaderChannel);
                    try (Channel syncLockChannel = leaderChannel.cloneChannel()) {
                        proxy(syncLockChannel).syncLock(null, ID);
                    }
                    proxy(leaderChannel).connect(null, ID, DingoConfiguration.location());
                    leaderChannel.setCloseListener(ch -> participantChannels.remove(leaderId));
                    this.leaderId = leaderId;
                    lockService.watchLock(currentLock, this::retryLock);
                } catch (Exception e) {
                    leaderChannel.close();
                    throw e;
                }
                log.info("Current {}, leader: {}.", ID, leaderId);
                listen(leaderLocation);
                needLock = false;
            } else {
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
        return leaderChannel.cloneChannel();
    }

    public boolean isReady() {
        return isLeader() || leaderId != null;
    }

    private void listen(Location location) {
        ListenService listenService = ListenService.getDefault();
        listenService.listen(
            createTableCommonId,
            null,
            location,
            msg -> MetaService.ROOT.cache.invalidSchema(new String(msg.content()).split("\\.")[0]),
            () -> log.info("Close listen create table {}", location)
        );
        listenService.listen(
            dropTableCommonId,
            null,
            location,
            msg -> {
                MetaService.ROOT.cache.invalidTable(new String(msg.content()));
                MetaService.ROOT.cache.invalidSchema(new String(msg.content()).split("\\.")[0]);
            },
            () -> log.info("Close listen drop table {}", location)
        );
        listenService.listen(
            createSchemaCommonId,
            null,
            location,
            msg -> MetaService.ROOT.cache.invalidMetaServices(),
            () -> log.info("Close listen create schema {}", location)
        );
        listenService.listen(
            dropSchemaCommonId,
            null,
            location,
            msg -> {
                MetaService.ROOT.cache.invalidMetaServices();
                MetaService.ROOT.cache.invalidSchema(new String(msg.content()));
            },
            () -> log.info("Close listen drop schema {}", location)
        );
    }

    @Override
    public void connect(Channel channel, CommonId serverId, Location location) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        Optional.ifPresent(participantChannels.put(serverId, channel), Channel::close);
        participantLocations.put(serverId, location);
        log.info("Participant {} join.", serverId);
        channel.setCloseListener(ch -> {
            participantChannels.remove(serverId);
            participantLocations.remove(serverId);
            log.info("Participant {} leave.", serverId);
        });
        participantJoinListener.accept(new Message(serverId.encode()));
    }

    @Override
    public Location getLocation(CommonId serverId) {
        return participantLocations.get(serverId);
    }

    private MetaServiceApi proxy(Location location) {
        return apis.proxy(MetaServiceApi.class, location);
    }

    private MetaServiceApi proxy(Channel channel) {
        return apis.proxy(MetaServiceApi.class, channel);
    }

    @Override
    @SneakyThrows
    public void syncLock(Channel channel, CommonId serverId) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (channel == null) {
            throw new RuntimeException("Unregister participant " + serverId);
        }
        for (TableLock tableLock : TableLockService.getDefault().getTableLocks()) {
            try (Channel ch = channel.cloneChannel()) {
                proxy(ch).lockTable(null, tableLock.lockTs, tableLock);
            }
        }
    }

    @Override
    @SneakyThrows
    public void getLockChannel(Channel channel, TableLock lock) {
        if (!isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        TableLock localLocked = TableLockService.getDefault().getTableLock(lock.getTableId());
        if (localLocked == null || !lock.serverId.equals(localLocked.serverId) || localLocked.compareTo(lock) != 0) {
            throw new RuntimeException("Not found lock.");
        }
        localLocked.unlockFuture.whenCompleteAsync((v, e) -> channel.close());
    }

    @Override
    @SneakyThrows
    public void lockTable(long requestId, TableLock lock) {
        if (isLeader()) {
            broadcastTableLock(requestId, lock);
        } else if (lock.serverId.equals(ID)) {
            try (Channel channel = leaderChannel.cloneChannel()) {
                MetaServiceApi metaServiceApi = proxy(channel);
                metaServiceApi.lockTable(null, requestId, lock);
            }
        }
    }

    @Override
    @SneakyThrows
    public void lockTable(Channel ch, long requestId, TableLock lock) {
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
        io.dingodb.store.proxy.service.TableLockService.INSTANCE.lock(lock);
        Channel lockChannel = participantChannels.get(lock.serverId);
        try {
            if (lockChannel == null) {
                try (Channel leaderChannel = this.leaderChannel.cloneChannel()) {
                    lockChannel = NetService.getDefault().newChannel(proxy(leaderChannel).getLocation(lock.serverId));
                    proxy(lockChannel).connect(null, lock.serverId, DingoConfiguration.location());
                    participantChannels.put(lock.serverId, lockChannel);
                }
            }
            lockChannel = lockChannel.cloneChannel();
            proxy(lockChannel).getLockChannel(null, lock);
            lockChannel.setCloseListener(channel -> unlockFuture.complete(null));
            lockFuture.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            lockFuture.cancel(true);
            unlockFuture.complete(null);
            Optional.ifPresent(lockChannel, Channel::close);
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
                proxy(channel).lockTable(null, requestId, lock);
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
            createTableListener.accept(new Message((schema + "." + table).getBytes(StandardCharsets.UTF_8)));
            MetaService.ROOT.cache.invalidSchema(schema);
            log.info("Create table {}.{}", schema, table);
        } else {
            try (Channel channel = leaderChannel.cloneChannel()) {
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
            dropTableListener.accept(new Message((schema + "." + table).getBytes(StandardCharsets.UTF_8)));
            MetaService.ROOT.cache.invalidTable(schema, table);
            MetaService.ROOT.cache.invalidSchema(schema);
        } else {
            try (Channel channel = leaderChannel.cloneChannel()) {
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
            createSchemaListener.accept(new Message(schema.getBytes(StandardCharsets.UTF_8)));
            MetaService.ROOT.cache.invalidMetaServices();
        } else {
            try (Channel channel = leaderChannel.cloneChannel()) {
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
            dropSchemaListener.accept(new Message(schema.getBytes(StandardCharsets.UTF_8)));
            MetaService.ROOT.cache.invalidMetaServices();
            MetaService.ROOT.cache.invalidSchema(schema);
        } else {
            try (Channel channel = leaderChannel.cloneChannel()) {
                proxy(channel).dropSchema(requestId, schema, request);
            }
        }
    }
}
