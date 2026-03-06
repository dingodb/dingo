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

package io.dingodb.server.executor.schedule;

import com.google.common.collect.Ordering;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.GlobalMemoryPool;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolListener;
import io.dingodb.common.memory.MemorySetting;
import io.dingodb.common.memory.QueryMemoryPool;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.impl.TaskManagerImpl;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.params.RevokerParams;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.common.memory.MemorySetting.DEFAULT_ALLOCATOR_SIZE;
import static io.dingodb.common.memory.MemorySetting.DEFAULT_LESS_REVOKE_BYTES;
import static io.dingodb.common.memory.MemorySetting.DEFAULT_MEMORY_REVOKING_TARGET;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class MemoryRevokingScheduler {

    private ScheduledFuture<?> scheduledFuture;

    private final MemoryPoolListener memoryPoolListener;

    private final AtomicBoolean checkPending = new AtomicBoolean();
    private GlobalMemoryPool memoryPool;

    public MemoryRevokingScheduler() {
        this.memoryPool = MemoryManager.getInstance().getGlobalMemoryPool();
        this.memoryPoolListener = (targetMemoryPool, target) -> onMemoryReserved(targetMemoryPool, target);
    }

    public void start() {
        registerPeriodicCheck();
        memoryPool.setMaxElasticMemory(MemorySetting.DEFAULT_MEMORY_REVOKING_THRESHOLD);
        memoryPool.addListener(memoryPoolListener);
    }

    private void registerPeriodicCheck() {
        this.scheduledFuture = Executors.scheduleWithFixedDelay("memoryRevokingScheduler", () -> {
            try {
                if (checkPending.compareAndSet(false, true)) {
                    runMemoryRevoking(true, DEFAULT_MEMORY_REVOKING_TARGET);
                }
            } catch (Throwable e) {
                log.error("Error requesting system memory revoking", e);
            }
        }, 1, 1, SECONDS);
    }

    private void onMemoryReserved(MemoryPool memoryPool, double target) {
        if (checkPending.compareAndSet(false, true)) {
            log.info("release the memory actively for " + memoryPool.getFullName());
            Executors.execute("runMemoryRevoking", () -> {
                try {
                    runMemoryRevoking(false, target);
                } catch (Throwable e) {
                    log.info("Error requesting memory revoking", e);
                }
            });
        }
    }

    private synchronized void runMemoryRevoking(boolean forceNotify, double target) {
        if (checkPending.getAndSet(false)) {
            if (memoryPool.getRevocableBytes() > 0) {
                List<Task> revocableTasks = new ArrayList<>();
                List<Task> taskManagerList = TaskManagerImpl.INSTANCE.getAllTasks();
                for (Task task : taskManagerList) {
                    MemoryPool queryMemoryPool = task.getMemoryPool();
                    if (queryMemoryPool != null && queryMemoryPool.getRevocableBytes() > 0) {
                        revocableTasks.add(task);
                    }
                }
                requestForceRevokingForQuery(revocableTasks);
                if (memoryRevokingNeeded(memoryPool)) {
                    log.info("GlobalMemory is used much more memory: used " + memoryPool.getMemoryUsage() + " total " +
                        memoryPool.getMaxLimit());

                    memoryPool.resetNeedMemoryRevoking();
                }
            }
        }
    }

    private boolean memoryRevokingNeeded(GlobalMemoryPool memoryPool) {
        boolean hasRevokeMemory = memoryPool.getRevocableBytes() > 0;
        boolean queryNeedMemory =
            (memoryPool.getBlockedFuture() != null && !memoryPool.getBlockedFuture().isDone()) || (
                memoryPool.getTrySettableFuture() != null && !memoryPool.getTrySettableFuture().isDone());

        return (memoryPool.getFreeBytes() <= memoryPool.getMaxLimit() * (1.0 -
            MemorySetting.DEFAULT_MEMORY_REVOKING_THRESHOLD) || memoryPool.isNeedMemoryRevoking()
            || queryNeedMemory) && hasRevokeMemory;
    }

    public void requestForceRevokingForQuery(List<Task> tasks) {
        // 注意一个sql中出现多个revoker的情况

        // step1 : 找到所有的sql
        // step2 : 找到每个的每个revoker，一个sql可能有多个revoker
        // step3 : 给sql中多个revoker排序
        // step4 : 拿到revoker的memoryAllocatedCtx 并且标记需要revoke
        Map<String, List<RevokerParams>> revokerParamsMap = new HashMap<>();
        Map<String, Long> queryToMinRequests = new HashMap<>();
        for (Task task : tasks) {
            QueryMemoryPool queryMemoryPool = (QueryMemoryPool) task.getMemoryPool();
            String queryId = queryMemoryPool.getName();
            if (!queryMemoryPool.isDestoryed()) {
                boolean needQueryRequest = false;
                if (queryMemoryPool.getBlockedFuture() != null && !queryMemoryPool.getBlockedFuture().isDone()) {
                    needQueryRequest = true;
                } else if (queryMemoryPool.getTrySettableFuture() != null
                    && !queryMemoryPool.getTrySettableFuture().isDone()) {
                    needQueryRequest = true;
                }
                if (needQueryRequest) {
                    List<RevokerParams> revokerParamsList = task.getVertexes().values().stream()
                        .filter(vertex -> vertex.getParam() instanceof RevokerParams)
                        .map(vertex -> (RevokerParams)vertex.getParam())
                        .toList();
                    long realRevokeBytes = Math.max((long) (
                            memoryPool.getMaxLimit() * (1.0 - DEFAULT_MEMORY_REVOKING_TARGET)
                                - memoryPool.getFreeBytes()),
                        memoryPool.getMinRequestMemory());
                    revokerParamsMap.put(queryId, revokerParamsList);
                    queryToMinRequests.put(queryId, realRevokeBytes);
                }
            }
        }
        for (Map.Entry<String, List<RevokerParams>> entry : revokerParamsMap.entrySet()) {
            LogUtils.info(log, "requestRevokingFor task start");
            String queryId = entry.getKey();
            long realRevokeBytes = queryToMinRequests.get(queryId);
            // real want revoke memory for query
            if (realRevokeBytes <= DEFAULT_LESS_REVOKE_BYTES / 8) {
                realRevokeBytes = DEFAULT_LESS_REVOKE_BYTES / 8;
            }

            AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(realRevokeBytes);
            //List<TaskContext> sortTaskContexts = entry.getValue().stream().sorted(
            //    TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE).collect(Collectors.toList());

            for (RevokerParams revokerParams : entry.getValue()) {
                requestRevokingForTask(
                    revokerParams, DEFAULT_ALLOCATOR_SIZE, remainingBytesToRevokeAtomic);
            }
        }

    }

    private static final Ordering<Task> TASK_ORDER_BY_REVOCABLE_MEMORY_SIZE =
        Ordering.natural().onResultOf(taskContext -> taskContext.getMemoryPool().getRevocableBytes());

    private static final Ordering<OperatorMemoryAllocatorCtx> OPERATOR_ORDER_BY_REVOCABLE_MEMORY_SIZE =
        Ordering.natural().onResultOf(memoryContext -> memoryContext.getRevocableAllocated());

    private void requestRevokingForTask(RevokerParams revokerParams,
                                        long guaranteedMemoryBytes, AtomicLong remainingBytesToRevokeAtomic) {
        if (remainingBytesToRevokeAtomic.get() <= 0) {
            return;
        }
        // get memoryAllocator ctx
        // get revocable allocated > guaranteedMemoryBytes
        // requestMemoryRevokingOrReturnRevokingBytes -> remainingBytes--
        OperatorMemoryAllocatorCtx memoryAllocatorCtx = revokerParams.getMemoryAllocatorCtx();
        if (memoryAllocatorCtx.getRevocableAllocated() > guaranteedMemoryBytes) {
            long revokeSize =
                memoryAllocatorCtx.requestMemoryRevokingOrReturnRevokingBytes();
            if (revokeSize > 0) {
                remainingBytesToRevokeAtomic.addAndGet(-revokeSize);
                if (log.isDebugEnabled()) {
                    log.debug("memoryPool=" + memoryAllocatorCtx.getName()
                        + ": requested revoking "
                        + revokeSize + "; remaining " + remainingBytesToRevokeAtomic.get());
                }
            }
        }
    }

    public void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }

}
