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

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.transaction.api.LockType.RANGE;

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

    private final Map<CommonId, TableLocks> tableLocks = new HashMap<>();
    private final LinkedRunner runner = new LinkedRunner("lock");
    private final HashSet<io.dingodb.transaction.api.TableLock> waitLocks = new HashSet<>();

    @Override
    public List<TableLock> allTableLocks() {
        return null;
    }

    @Override
    public List<TableLock> getTableLocks() {
        return null;
    }

    @Override
    public TableLock getTableLock(CommonId tableId) {
        return null;
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

}
