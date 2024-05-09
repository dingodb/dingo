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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.store.proxy.meta.MetaServiceApiImpl;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.transaction.api.LockType.RANGE;
import static io.dingodb.transaction.api.LockType.TABLE;

@Slf4j
public class TableLockService implements io.dingodb.transaction.api.TableLockService {

    @AutoService(TableLockServiceProvider.class)
    public static final class LockServiceProvider implements TableLockServiceProvider {

        @Override
        public TableLockService get() {
            return INSTANCE;
        }
    }

    public static final TableLockService INSTANCE = new TableLockService();

    private class TableLocks {
        final List<io.dingodb.transaction.api.TableLock> locked = new LinkedList<>();
        final TreeSet<io.dingodb.transaction.api.TableLock> lockQueue = new TreeSet<>();
        final LinkedRunner runner = new LinkedRunner("lock");
    }

    private final Map<CommonId, TableLocks> locks = new HashMap<>();
    private final LinkedRunner runner = new LinkedRunner("lock");
    private final HashSet<io.dingodb.transaction.api.TableLock> waitLocks = new HashSet<>();

    private final Map<CommonId, TableLock> tableLocks = new ConcurrentHashMap<>();

    private TableLockService() {
        ApiRegistry.getDefault().register(io.dingodb.transaction.api.TableLockService.class, this);
    }

    @Override
    public TableLock getTableLock(CommonId tableId) {
        return tableLocks.get(tableId);
    }

    @Override
    public List<TableLock> getTableLocks() {
        return new ArrayList<>(tableLocks.values());
    }

    @Override
    public List<TableLock> allTableLocks() {
        return locks.values().stream()
            .flatMap($ -> Stream.concat($.locked.stream(), $.lockQueue.stream()))
            .collect(Collectors.toList());
    }

    @Override
    public void lock(TableLock lock) {
        if (MetaServiceApiImpl.INSTANCE.isReady()) {
            doLock(lock);
            return;
        }
        throw new RuntimeException("Offline, please wait and retry.");
    }

    public void doLock(TableLock lock) {
        runner.forceFollow(() -> {
            TableLocks tableLocks = this.locks.computeIfAbsent(lock.tableId, k -> new TableLocks());
            tableLocks.runner.forceFollow(() -> {
                tableLocks.lockQueue.add(lock);
                waitLocks.add(lock);
            });
            tableLocks.runner.forceFollow(() -> lock(lock.tableId, lock.lockTs, lock.currentTs));
        });
        LogUtils.info(log, "doLock end table:{}", lock.toString());
    }

    public void unlock(io.dingodb.transaction.api.TableLock lock) {
        runner.forceFollow(() -> {
            locks.get(lock.getTableId()).runner.forceFollow(() -> {
                locks.get(lock.tableId).locked.remove(lock);
                LogUtils.info(log, "Unlocked: {}", lock);
            });
        });
    }

    private void lock(CommonId tableId, long lockTs, long currentTs) {
        LogUtils.info(log, "lock tableId:{}, lockTs:{}, currentTs:{}", tableId, lockTs, currentTs);
        TableLocks tableLocks = this.locks.get(tableId);
        io.dingodb.transaction.api.TableLock lock = tableLocks.lockQueue.first();
        LogUtils.info(log, "tableLocks.lockQueue.first:{}", lock);
        if (lock == null) {
            LogUtils.error(log, "Poll {} first wait lock null.", tableId);
            return;
        }
        List<io.dingodb.transaction.api.TableLock> locks = this.locks.get(lock.tableId).locked;
        if (locks.size() > 0 && log.isInfoEnabled()) {
            StringJoiner lockJoiner = new StringJoiner(",\n\t");
            locks.forEach($ -> lockJoiner.add($.serverId + "|" + $.type + "|" + $.lockTs + "|" + $.currentTs));
            LogUtils.info(log, "{} lock not empty, locks: [\n\t{}\n]", tableId, lockJoiner);
        }
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
            if (lock.type == TABLE || lock.type == RANGE) {
                this.tableLocks.put(tableId, lock);
                lock.unlockFuture.whenCompleteAsync((v, r) -> this.tableLocks.remove(tableId), Executors.LOCK_FUTURE_POOL);
                try {
                    MetaServiceApiImpl.INSTANCE.lockTable(lock.lockTs, lock);
                } catch (Exception e) {
                    if (e instanceof TimeoutException) {
                        LogUtils.trace(log, "Lock table {} error.", tableId, e);
                    } else {
                        LogUtils.error(log, "Lock table {} error.", tableId, e);
                    }
                    locked = false;
                }
            }
        }
        if (locked) {
            future.complete(true);
            locks.add(lock);
            tableLocks.lockQueue.remove(lock);
            waitLocks.remove(lock);
            lock.unlockFuture.whenCompleteAsync((v, e) -> unlock(lock), Executors.LOCK_FUTURE_POOL);
            LogUtils.info(log, "Locked {}", lock);
        } else {
            if (lock.lockFuture.isCancelled()) {
                tableLocks.lockQueue.remove(lock);
                LogUtils.info(log, "Lock cancel {}", lock);
                return;
            }
            if (lock.lockFuture.isCompletedExceptionally()) {
                tableLocks.lockQueue.remove(lock);
                LogUtils.info(log, "Lock error {}", lock);
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            LogUtils.info(log, "locked is false Lock {}", lock);
            tableLocks.runner.forceFollow(() -> lock(lock.tableId, lock.getLockTs(), lock.getCurrentTs()));
        }
    }

}
