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
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.service.LockService;
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
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
    private LockService.Lock lock;

    private class TableLocks {
        final List<io.dingodb.transaction.api.TableLock> locked = new LinkedList<>();
        final TreeSet<io.dingodb.transaction.api.TableLock> lockQueue = new TreeSet<>();
        final LinkedRunner runner = new LinkedRunner("lock");
    }

    private final Map<CommonId, TableLocks> tableLocks = new HashMap<>();
    private final LinkedRunner runner = new LinkedRunner("lock");
    private final HashSet<io.dingodb.transaction.api.TableLock> waitLocks = new HashSet<>();

    private final Map<CommonId, TableLock> tableOrRangeTables = new ConcurrentHashMap<>();

    private TableLockService() {
    }

    public TableLock getTableOrRangeLock(CommonId tableId) {
        return tableOrRangeTables.get(tableId);
    }

    public List<TableLock> getTableOrRangeLocks() {
        return new ArrayList<>(tableOrRangeTables.values());
    }

    public void lock(TableLock lock) {
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
            if (lock.type == TABLE || lock.type == RANGE) {
                tableOrRangeTables.put(tableId, lock);
                lock.unlockFuture.whenCompleteAsync((v, r) -> tableOrRangeTables.remove(tableId));
                try {
                    MetaServiceApiImpl.INSTANCE.lockTable(lock.lockTs, lock);
                } catch (Exception e) {
                    log.error("Lock table {} error.", tableId, e);
                    locked = false;
                }
            }
        }
        if (locked) {
            future.complete(true);
            locks.add(lock);
            tableLocks.lockQueue.pollFirst();
            waitLocks.remove(lock);
            lock.unlockFuture.whenCompleteAsync((v, e) -> unlock(lock));
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
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            tableLocks.runner.forceFollow(() -> lock(lock.tableId));
        }
    }

}
