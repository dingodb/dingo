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

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.net.Channel;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.sdk.service.LockService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.api.TableLockApi;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.transaction.api.LockType.RANGE;
import static io.dingodb.transaction.api.LockType.TABLE;

@Slf4j
public class TableLockService implements TableLockApi, io.dingodb.transaction.api.TableLockService {

    @AutoService(TableLockServiceProvider.class)
    public static final class LockServiceProvider implements TableLockServiceProvider {

        @Override
        public TableLockService get() {
            return INSTANCE;
        }
    }

    public static final TableLockService INSTANCE = new TableLockService();
    private final LockService lockService;
    private LockService.Lock lock;

    private class TableLocks {
        final List<io.dingodb.transaction.api.TableLock> locked = new LinkedList<>();
        final TreeSet<io.dingodb.transaction.api.TableLock> lockQueue = new TreeSet<>();
        final LinkedRunner runner = new LinkedRunner("lock");
    }

    private final Map<CommonId, TableLocks> tableLocks = new HashMap<>();
    private final LinkedRunner runner = new LinkedRunner("lock");
    private final HashSet<io.dingodb.transaction.api.TableLock> waitLocks = new HashSet<>();

    private final Map<Location, Channel> channels = new HashMap<>();
    private final LinkedRunner apiRunner = new LinkedRunner("lock-api");

    private Location leader;
    private Channel leaderChannel;

    private TableLockService() {
        ApiRegistry.getDefault().register(TableLockApi.class, this);
        LockService lockService = new LockService("table-lock", Configuration.coordinators());
        this.lockService = lockService;
        lock = this.lockService.newLock(DingoConfiguration.location().url());
        register();
    }

    public TableLockService(String coordinators) {
        ApiRegistry.getDefault().register(TableLockApi.class, this);
        lockService = new LockService("table-lock", coordinators);
        lock = lockService.newLock();
        register();
    }

    public void lock(io.dingodb.transaction.api.TableLock lock) {
        runner.forceFollow(() -> {
            TableLocks tableLocks = this.tableLocks.computeIfAbsent(lock.tableId, k -> new TableLocks());
            tableLocks.runner.forceFollow(() -> {
                tableLocks.lockQueue.add(lock);
                waitLocks.add(lock);
            });
            tableLocks.runner.forceFollow(() -> lock(lock.tableId));
        });
    }

    public void unlock(io.dingodb.transaction.api.TableLock lock) {
        runner.forceFollow(() -> {
            tableLocks.get(lock.getTableId()).runner.forceFollow(() -> {
                tableLocks.get(lock.tableId).locked.remove(lock);
                if (log.isDebugEnabled()) {
                    log.debug("Unlocked: {}", lock);
                }
            });
        });
    }

    private void lock(CommonId tableId) {
        TableLocks tableLocks = this.tableLocks.get(tableId);
        io.dingodb.transaction.api.TableLock lock = tableLocks.lockQueue.first();
        if (lock == null) {
            log.error("Poll {} first wait lock null.", tableId);
            return;
        }
        List<io.dingodb.transaction.api.TableLock> locks = this.tableLocks.get(lock.tableId).locked;
        CompletableFuture<Boolean> future = lock.lockFuture;
        boolean locked = locks.isEmpty();
        if (!locked) {
            switch (lock.type) {
                case TABLE:
                    // lock table need locks empty
                    break;
                case RANGE:
                    if (locks.stream().noneMatch($1 -> $1.type != RANGE)) {
                        List<byte[]> keys = Stream.concat(
                                Stream.of(lock.start, lock.end), locks.stream().flatMap($ -> Stream.of($.start, $.end))
                            ).sorted(ByteArrayUtils::compare)
                            .distinct()
                            .collect(Collectors.toList());
                        int i = keys.indexOf(lock.start);
                        locked = keys.size() == locks.size() * 2 + 2
                            && (i & 1) == 0
                            && Arrays.equals(keys.get(i + 1), lock.end);
                    }
                    break;
                case ROW:
                    locked = locks.stream().noneMatch($ -> $.type != LockType.ROW);
                    break;
                default:
                    lock.lockFuture.completeExceptionally(new RuntimeException("Not support type."));
            }
        }
        if (locked) {
            if (lock.type == TABLE) {
                broadcastLock(lock, null);
            }
            future.complete(true);
            locks.add(lock);
            tableLocks.lockQueue.pollFirst();
            waitLocks.remove(lock);
            lock.unlockFuture.whenComplete((v, e) -> unlock(lock));
            if (log.isDebugEnabled()) {
                log.debug("Locked {}", lock);
            }
        } else {
            if (lock.lockFuture.isCancelled()) {
                tableLocks.lockQueue.pollFirst();
                if (log.isDebugEnabled()) {
                    log.debug("Lock cancel {}", lock);
                }
                return;
            }
            if (lock.lockFuture.isCompletedExceptionally()) {
                tableLocks.lockQueue.pollFirst();
                if (log.isDebugEnabled()) {
                    log.debug("Lock error {}", lock);
                }
                return;
            }
            tableLocks.runner.forceFollow(() -> lock(lock.tableId));
            if (waitLocks.size() == 1) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            }
        }
    }

    public void register() {
        while (!lock.tryLock()) {
            try {
                Kv kv = lockService.listLock().stream()
                    .filter($ -> $.getKv().getValue().length != 0)
                    .min(Comparator.comparingLong(Kv::getCreateRevision)).get();
                Location location = Location.parseUrl(new String(kv.getKv().getValue()));
                Channel channel = NetService.getDefault().newChannel(location);
                TableLockApi tableLockApi = ApiRegistry.getDefault().proxy(TableLockApi.class, channel);
                tableLockApi.register(null, DingoConfiguration.location());
                this.leader = location;
                this.leaderChannel = channel;
                return;
            } catch (Exception ignore) {
            }
        }
    }

    public void register(Channel channel, Location location) {
        apiRunner.forceFollow(() -> {
            channels.put(location, channel);
            channel.setCloseListener(ch -> channels.remove(location));
        });
    }

    @Override
    public void lock(Channel channel, io.dingodb.transaction.api.TableLock lock, int ttl) {
        if (lock.type != TABLE && lock.type != RANGE) {
            throw new RuntimeException("Supported only table and range.");
        }
        CompletableFuture<Boolean> lockFuture = new CompletableFuture<>();
        CompletableFuture<Void> unlockFuture = new CompletableFuture<>();
        lock = io.dingodb.transaction.api.TableLock.builder()
            .lockTs(lock.lockTs)
            .currentTs(lock.currentTs)
            .tableId(lock.tableId)
            .type(lock.type)
            .lockFuture(lockFuture)
            .unlockFuture(unlockFuture)
            .build();
        lock(lock);
        try {
            channel.setCloseListener(ch -> unlockFuture.complete(null));
            lockFuture.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            lockFuture.cancel(true);
            throw new RuntimeException("Timeout.");
        } catch (Exception e) {
            lockFuture.cancel(true);
            throw new RuntimeException(e);
        }
    }

    private void broadcastLock(TableLock lock, Location requestLockLocation) {
        if (this.lock.tryLock()) {
            List<Location> broadcastList = lockService.listLock().stream()
                .map(Kv::getKv)
                .map(KeyValue::getValue)
                .filter($ -> $.length == 0)
                .map(String::new)
                .map(Location::parseUrl)
                .filter($ -> !$.equals(requestLockLocation))
                .filter($ -> !$.equals(DingoConfiguration.location())).collect(Collectors.toList());
            for (Location location : broadcastList) {
                Channel channel = NetService.getDefault().newChannel(location);
                try {
                    TableLockApi tableLockApi = ApiRegistry.getDefault().proxy(TableLockApi.class, channel);
                    tableLockApi.lock(null, lock, 1);
                    lock.unlockFuture.thenRun(channel::close);
                } catch (Exception e) {
                    channel.close();
                    throw new RuntimeException(e);
                }
            }
        } else {
            Channel channel = NetService.getDefault().newChannel(leader);
            lock.unlockFuture.thenRun(channel::close);
            TableLockApi tableLockApi = ApiRegistry.getDefault().proxy(TableLockApi.class, channel);
            while (true) {
                try {
                    tableLockApi.lock(null, lock, 3);
                    return;
                } catch (Exception ignore) {
                    if (lock.lockFuture.isCancelled()) {
                        channel.close();
                        return;
                    }
                }
            }
        }
    }

}
