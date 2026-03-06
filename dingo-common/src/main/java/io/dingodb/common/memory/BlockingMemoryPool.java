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

package io.dingodb.common.memory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;
import static io.dingodb.common.memory.MemorySetting.DEFAULT_MEMORY_REVOKING_THRESHOLD;

@Slf4j
public abstract class BlockingMemoryPool extends MemoryPool {
    @GuardedBy("this")
    private long tryMinRequestSize = 0;

    @GuardedBy("this")
    private long minRequestSize = 0;

    private long maxRequestSize = 0;

    @GuardedBy("this")
    private SettableFuture<?> settableFuture;

    @GuardedBy("this")
    private SettableFuture<?> trySettableFuture;

    @GuardedBy("this")
    private boolean needMemoryRevoking = false;

    protected long maxElasticMemory;

    protected boolean blockFlag;

    public BlockingMemoryPool(String name, long maxLimit, MemoryPool parent, MemoryType memoryType) {
        super(name, maxLimit, parent, memoryType);
        setMaxElasticMemory(DEFAULT_MEMORY_REVOKING_THRESHOLD);
    }

    public void setMaxElasticMemory(double maxElasticThreshold) {
        this.maxElasticMemory = (long) (this.maxLimit * maxElasticThreshold);
    }

    @Override
    public void setMaxLimit(long memoryLimit) {
        this.maxLimit = memoryLimit;
        setMaxElasticMemory(DEFAULT_MEMORY_REVOKING_THRESHOLD);
    }

    @Override
    protected ListenableFuture<?> block(ListenableFuture<?> parent, long size) {
        if (parent != null && !parent.isDone()) {
            if (inheritParentFuture()) {
                return parent;
            } else {
                return NOT_BLOCKED;
            }
        } else {
            if (this.getMemoryType() == MemoryType.GLOBAL) {
                long maxRequestSizeTmp = reservedBytes + revocableBytes;
                if (maxRequestSizeTmp > maxRequestSize) {
                    maxRequestSize = maxRequestSizeTmp;
                }
            }
            if (reservedBytes + revocableBytes > maxElasticMemory && ScopeVariables.enableSpill()) {
                log.info("The query use much more memory for the memory pool: " + name);
                if (minRequestSize <= 0 || minRequestSize > size) {
                    minRequestSize = size;
                }
                if (revocableBytes >= minRequestSize) {
                    //存在可释放的内存的时候，才阻塞
                    //return the blocked future after the allocated memory exceed the maxElasticMemory.
                    if (settableFuture == null || settableFuture.isDone()) {
                        settableFuture = SettableFuture.create();
                        this.blockFlag = true;
                    }
                    checkState(!settableFuture.isDone(), "future is already completed");
                    this.needMemoryRevoking = true;
                    requestMemoryRevoke();
                    LogUtils.info(log, "memoryPool:{}, get block future:{}", this.getFullName(), settableFuture);
                    return settableFuture;
                } else {
                    // Don't blocked the current query if revocableBytes is less than the minRequestSize.
                    return NOT_BLOCKED;
                }
            } else {
                return NOT_BLOCKED;
            }
        }
    }

    @Override
    protected void tryBlock(MemoryAllocateFuture allocFuture, long size, boolean reserved) {
        if (size > maxElasticMemory) {
            outOfMemory(fullName, getMemoryUsage(), size, maxElasticMemory, reserved);
        }

        if (tryMinRequestSize <= 0 || tryMinRequestSize > size) {
            tryMinRequestSize = size;
        }

        if ((revocableBytes >= tryMinRequestSize || reserved) && ScopeVariables.enableSpill()) {
            log.info("The query use much more memory for the memory pool: " + name);
            if (trySettableFuture == null || trySettableFuture.isDone()) {
                trySettableFuture = SettableFuture.create();
                this.blockFlag = true;
            }
            allocFuture.setAllocateFuture(trySettableFuture);
            this.needMemoryRevoking = true;
            requestMemoryRevoke();
        } else {
            outOfMemory(fullName, getMemoryUsage(), size, maxElasticMemory, reserved);
        }
    }

    @Override
    protected synchronized void notifyBlockedQuery() {
        if (settableFuture == null && trySettableFuture == null) {
            return;
        }

        long availableBytes = maxElasticMemory - reservedBytes - revocableBytes;
        if (availableBytes >= minRequestSize && settableFuture != null && !settableFuture.isDone()) {
            minRequestSize = 0;
            LogUtils.info(log, "block settable set null, name:{}", this.getFullName());
            settableFuture.set(null);
        }
        if (availableBytes >= tryMinRequestSize && trySettableFuture != null && !trySettableFuture.isDone()) {
            tryMinRequestSize = 0;
            trySettableFuture.set(null);
        }
    }

    @Override
    protected long getTryMaxLimit() {
        return maxElasticMemory;
    }

    protected boolean inheritParentFuture() {
        return true;
    }

    @Override
    public void destroy() {
        if (settableFuture != null) {
            settableFuture.set(null);
        }

        if (trySettableFuture != null) {
            trySettableFuture.set(null);
        }
        super.destroy();
    }

    public synchronized boolean isNeedMemoryRevoking() {
        return needMemoryRevoking;
    }

    public synchronized void resetNeedMemoryRevoking() {
        this.needMemoryRevoking = false;
    }

    public ListenableFuture<?> getBlockedFuture() {
        return settableFuture;
    }

    public SettableFuture<?> getTrySettableFuture() {
        return trySettableFuture;
    }

    public long getMinRequestMemory() {
        return tryMinRequestSize + minRequestSize;
    }

    public boolean isBlockFlag() {
        return blockFlag;
    }
}
