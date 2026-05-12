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

package io.dingodb.exec.memory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.error.MemoryNotEnoughException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryNotFuture;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.dingodb.common.memory.MemorySetting.DEFAULT_ALLOCATOR_SIZE;
import static io.dingodb.common.memory.MemorySetting.DEFAULT_LESS_REVOKE_BYTES;

@Slf4j
public class OperatorMemoryAllocatorCtx implements MemoryAllocatorCtx {
    private final MemoryPool memoryPool;

    private final AtomicLong reservedFree = new AtomicLong(0L);

    private final AtomicLong reservedAllocated = new AtomicLong(0L);

    private final AtomicLong revocableFree = new AtomicLong(0L);

    public AtomicInteger revocableCnt;

    private final AtomicLong revocableAllocated = new AtomicLong(0L);

    private final AtomicReference<MemoryNotFuture> allocateBytesFuture;

    private final boolean revocable;

    private SettableFuture<?> memoryRevokingRequestedFuture;

    public OperatorMemoryAllocatorCtx(MemoryPool memoryPool, boolean revocable) {
        this.memoryPool = memoryPool;
        this.allocateBytesFuture = new AtomicReference<>();
        this.allocateBytesFuture.set(MemoryNotFuture.create());
        this.allocateBytesFuture.get().set(null);
        this.revocable = revocable;
        this.revocableCnt = new AtomicInteger(0);
        if (this.revocable) {
            memoryRevokingRequestedFuture = SettableFuture.create();
        }
    }

    @Override
    public void allocateReservedMemory(long bytes) {
        long left = reservedFree.addAndGet(-bytes);
        if (left < 0) {
            // Align to block size
            long amount = -Math.floorDiv(left, DEFAULT_ALLOCATOR_SIZE) * DEFAULT_ALLOCATOR_SIZE;
            try {
                updateMemoryFuture(memoryPool.allocateReserveMemory(amount), allocateBytesFuture, false);
            } catch (MemoryNotEnoughException t) {
                reservedFree.addAndGet(bytes);
                throw t;
            }
            reservedFree.addAndGet(amount);
            reservedAllocated.addAndGet(amount);
        }
    }

    @Override
    public void allocateRevocableMemory(long bytes) {
        long left = revocableFree.addAndGet(-bytes);
        if (left < 0) {
            // Align to block size
            long amount = -Math.floorDiv(left, DEFAULT_ALLOCATOR_SIZE) * DEFAULT_ALLOCATOR_SIZE;
            try {
                updateMemoryFuture(memoryPool.allocateRevocableMemory(amount), allocateBytesFuture, false);
            } catch (MemoryNotEnoughException t) {
                revocableFree.addAndGet(bytes);
                throw t;
            }
            revocableFree.addAndGet(amount);
            revocableAllocated.addAndGet(amount);
        }
    }

    @Override
    public long getReservedAllocated() {
        return reservedAllocated.get();
    }

    @Override
    public long getRevocableAllocated() {
        return revocableAllocated.get();
    }

    @Override
    public long getAllAllocated() {
        return reservedAllocated.get() + revocableAllocated.get();
    }

    @Override
    public String getName() {
        return memoryPool.getName();
    }

    @Override
    public boolean isMemoryRevokingRequested() {
        if (!revocable) {
            return false;
        }
        return memoryRevokingRequestedFuture.isDone();
    }

    @Override
    public synchronized void resetMemoryRevokingRequested() {
        if (!revocable) {
            return;
        }
        SettableFuture<?> currentFuture = memoryRevokingRequestedFuture;
        if (!currentFuture.isDone()) {
            return;
        }
        memoryRevokingRequestedFuture = SettableFuture.create();
        LogUtils.debug(log, "resetMemoryRevokingRequested");
    }

    @Override
    public ListenableFuture<?> isWaitingForMemory() {
        return allocateBytesFuture.get();
    }

    @Override
    public void releaseRevocableMemory(long bytes, boolean immediately) {
        Preconditions.checkState(revocable, "Don't allocate memory in the reserved mode!");
        if (immediately) {
            long alreadyAllocated = revocableAllocated.get();
            if (alreadyAllocated - bytes <= revocableFree.get()) {
                memoryPool.freeRevocableMemory(revocableAllocated.getAndSet(0));
                revocableFree.set(0);
            } else {
                long actualFreeSize = Math.min(alreadyAllocated, bytes);
                revocableAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeRevocableMemory(actualFreeSize);
            }
        } else if (revocableAllocated.get() > revocableFree.addAndGet(bytes)) {
            if (revocableFree.get() >= DEFAULT_ALLOCATOR_SIZE) {
                long alreadyAllocated = revocableAllocated.get();
                long freeSize = revocableFree.getAndSet(0);
                long actualFreeSize = Math.min(alreadyAllocated, freeSize);
                revocableAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeRevocableMemory(actualFreeSize);
            }
        } else {
            memoryPool.freeRevocableMemory(revocableAllocated.getAndSet(0));
            revocableFree.set(0);
        }
    }

    @Override
    public void releaseReservedMemory(long bytes, boolean immediately) {
        if (immediately) {
            long alreadyAllocated = reservedAllocated.get();
            if (alreadyAllocated - bytes < reservedFree.get()) {
                memoryPool.freeReserveMemory(reservedAllocated.getAndSet(0));
                reservedFree.set(0);
            } else {
                long actualFreeSize = Math.min(alreadyAllocated, bytes);
                reservedAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeReserveMemory(actualFreeSize);
            }
        } else if (reservedAllocated.get() > reservedFree.addAndGet(bytes)) {
            if (reservedFree.get() >= DEFAULT_ALLOCATOR_SIZE) {
                long freeSize = reservedFree.getAndSet(0);
                long actualFreeSize = Math.min(revocableAllocated.get(), freeSize);
                reservedAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeReserveMemory(actualFreeSize);
            }
        } else {
            memoryPool.freeReserveMemory(reservedAllocated.getAndSet(0));
            reservedFree.set(0);
        }
    }

    private void updateMemoryFuture(ListenableFuture<?> memoryPoolFuture,
                                    AtomicReference<MemoryNotFuture> targetFutureReference, boolean isTry) {
        if (memoryPoolFuture != null && !memoryPoolFuture.isDone()
            && (isTry || getAllAllocated() > DEFAULT_LESS_REVOKE_BYTES / 8)) {
            //如果不是尝试性申请内存的话，则不阻塞 4MB 以下的算子
            MemoryNotFuture<?> currentMemoryFuture = targetFutureReference.get();
            if (currentMemoryFuture.isDone()) {
                MemoryNotFuture<?> settableFuture = MemoryNotFuture.create();
                targetFutureReference.set(settableFuture);
            }
            MemoryNotFuture<?> finalMemoryFuture = targetFutureReference.get();
            // Create a new future, so that this operator can un-block before the pool does, if it's moved to a new pool
            Futures.addCallback(memoryPoolFuture, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    finalMemoryFuture.set(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    finalMemoryFuture.set(null);
                }
            }, directExecutor());
        }
    }

    public synchronized long requestMemoryRevokingOrReturnRevokingBytes(long revokeFlag) {
        checkState(revocable, "requestMemoryRevoking for unRevocable operator");
        boolean alreadyRequested = isMemoryRevokingRequested();
        if (!alreadyRequested && revocableAllocated.get() > 0) {
            LogUtils.debug(log, "request memory revoking,name:{}, revocableAllocated:{}, revokeFlag:{}",
                this.getName(), revocableAllocated.get(), revokeFlag);
            memoryRevokingRequestedFuture.set(null);
            return revocableAllocated.get();
        }
        if (alreadyRequested) {
            return revocableAllocated.get();
        }
        return 0;
    }

    public synchronized SettableFuture<?> getMemoryRevokingRequestedFuture() {
        return memoryRevokingRequestedFuture;
    }

    @Override
    public void close() {
        this.memoryPool.destroy();
    }
}
