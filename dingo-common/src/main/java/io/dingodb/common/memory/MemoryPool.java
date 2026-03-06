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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.common.base.Preconditions;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.DingoErrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.dingodb.common.mysql.error.ErrorCode.ErrOutOfMemory;

@Slf4j
public class MemoryPool {

    protected static final ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    protected final String name;

    protected final AtomicLong memoryUsageStat = new AtomicLong();
    protected final MemoryPool parent;
    protected final Map<String, MemoryPool> children = new ConcurrentHashMap<>();
    protected final MemoryType memoryType;
    protected final String fullName;

    protected long maxLimit;
    protected volatile long maxMemoryUsage = 0L;
    protected long reservedBytes;
    protected long revocableBytes;

    protected AtomicBoolean destroyed = new AtomicBoolean(false);

    public MemoryPool(String name, long maxLimit, MemoryType memoryType) {
        this(name, maxLimit, null, memoryType);
    }

    public MemoryPool(String name, long maxLimit, MemoryPool parent, MemoryType memoryType) {
        this.name = name;
        this.fullName = parent != null ? parent.fullName + "/" + name : name;
        this.maxLimit = maxLimit;
        this.parent = parent;
        this.memoryType = memoryType;
    }

    public MemoryPool getOrCreatePool(String name) {
        return getOrCreatePool(name, maxLimit, MemoryType.OTHER);
    }

    public MemoryPool getOrCreatePool(MemoryType memoryType) {
        return this.getOrCreatePool(memoryType.getExtensionName(), maxLimit, memoryType);
    }

    public MemoryPool getOrCreatePool(String name, MemoryType memoryType) {
        return getOrCreatePool(name, maxLimit, memoryType);
    }

    public MemoryPool getOrCreatePool(MemoryType memoryType, long maxLimit) {
        return this.getOrCreatePool(memoryType.getExtensionName(), maxLimit, memoryType);
    }

    public MemoryPool getOrCreatePool(String name, long maxLimit, MemoryType memoryType) {
        Preconditions.checkArgument(!StringUtils.isEmpty(name) && maxLimit > 0);
        try {
            Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
        } catch (Throwable e) {
            throw e;
        }
        return children
            .computeIfAbsent(name, k -> MemoryPoolUtils.createNewPool(name, maxLimit, memoryType, this));
    }

    public ListenableFuture<?> allocateReserveMemory(long size) {
        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
        checkArgument(size >= 0, "bytes is negative");
        ListenableFuture<?> future = null;
        synchronized (this) {
            if (parent != null) {
                future = parent.allocateReserveMemory(size);
            }
            long nowReservedBytes = reservedBytes + size;
            long nowTotalUsage = nowReservedBytes + revocableBytes;
            if (nowTotalUsage > maxLimit) {
                if (parent != null) {
                    parent.freeReserveMemory(size);
                }
                outOfMemory(fullName, getMemoryUsage(), size, maxLimit, true);
            }

            if (nowTotalUsage > maxMemoryUsage) {
                maxMemoryUsage = nowTotalUsage;
            }
            reservedBytes = nowReservedBytes;
            future = block(future, size);
        }
        return future;
    }

    protected long getTryMaxLimit() {
        return maxLimit;
    }

    protected boolean tryAllocateReserveMemory(long size, MemoryAllocateFuture allocFuture) {
        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
        checkArgument(size >= 0, "bytes is negative");
        synchronized (this) {
            if (parent != null) {
                if (!parent.tryAllocateReserveMemory(size, allocFuture)) {
                    return false;
                }
            }
            long nowReservedBytes = reservedBytes + size;
            long nowTotalUsage = nowReservedBytes + revocableBytes;

            if (nowTotalUsage > getTryMaxLimit()) {
                if (parent != null) {
                    parent.freeReserveMemory(size);
                }
                tryBlock(allocFuture, size, true);
                return false;
            }

            allocFuture.setAllocateFuture(NOT_BLOCKED);
            if (nowTotalUsage > maxMemoryUsage) {
                maxMemoryUsage = nowTotalUsage;
            }
            reservedBytes = nowReservedBytes;
        }
        return true;
    }

    protected boolean tryAllocateRevocableMemory(long size, MemoryAllocateFuture allocFuture) {
        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
        checkArgument(size >= 0, "bytes is negative");
        synchronized (this) {
            if (parent != null) {
                if (!parent.tryAllocateRevocableMemory(size, allocFuture)) {
                    return false;
                }
            }
            long nowRevocableBytes = revocableBytes + size;
            long nowTotalUsage = nowRevocableBytes + reservedBytes;

            if (nowTotalUsage > getTryMaxLimit()) {
                if (parent != null) {
                    parent.freeRevocableMemory(size);
                }
                tryBlock(allocFuture, size, false);
                return false;
            }

            allocFuture.setAllocateFuture(NOT_BLOCKED);
            if (nowTotalUsage > maxMemoryUsage) {
                maxMemoryUsage = nowTotalUsage;
            }
            revocableBytes = nowRevocableBytes;
        }
        return true;
    }

//    protected boolean tryAllocateReserveMemory(long size, MemoryAllocateFuture allocFuture) {
//        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
//        checkArgument(size >= 0, "bytes is negative");
//        synchronized (this) {
//
//            long nowReservedBytes = reservedBytes + size;
//            long nowTotalUsage = nowReservedBytes + revocableBytes;
//            //allocate the memory itself firstly.
//            if (nowTotalUsage > getTryMaxLimit()) {
//                tryBlock(allocFuture, size);
//                return false;
//            } else {
//                if (parent != null) {
//                    boolean bSuccess = parent.tryAllocateReserveMemory(size, allocFuture);
//                    if (!bSuccess) {
//                        return false;
//                    }
//                }
//            }
//            allocFuture.setAllocateFuture(NOT_BLOCKED);
//            if (nowTotalUsage > maxMemoryUsage) {
//                maxMemoryUsage = nowTotalUsage;
//            }
//            reservedBytes = nowReservedBytes;
//        }
//        return true;
//    }

    public ListenableFuture<?> allocateRevocableMemory(long size) {
        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
        checkArgument(size >= 0, "bytes is negative");
        ListenableFuture<?> future = null;
        synchronized (this) {
            if (parent != null) {
                future = parent.allocateRevocableMemory(size);
            }
            long nowRevocableBytes = revocableBytes + size;

            long nowTotalUsage = reservedBytes + nowRevocableBytes;

            if (nowTotalUsage > maxLimit) {
                if (parent != null) {
                    parent.freeRevocableMemory(size);
                }
                outOfMemory(fullName, getMemoryUsage(), size, maxLimit, false);
            }
            if (nowTotalUsage > maxMemoryUsage) {
                maxMemoryUsage = nowTotalUsage;
            }
            revocableBytes = nowRevocableBytes;
            future = block(future, size);
        }
        return future;
    }

    public void freeReserveMemory(long size) {

        checkArgument(size >= 0, "bytes is negative");
        if (size == 0) {
            // Freeing zero bytes is a no-op
            return;
        }
        synchronized (this) {
            long releaseSize = Math.min(reservedBytes, size);
            reservedBytes -= releaseSize;
            if (parent != null) {
                parent.freeReserveMemory(releaseSize);
            }
            notifyBlockedQuery();
        }
    }

    public void freeRevocableMemory(long size) {
        checkArgument(size >= 0, "bytes is negative");
        if (size == 0) {
            // Freeing zero bytes is a no-op
            return;
        }
        synchronized (this) {
            long releaseSize = Math.min(revocableBytes, size);
            revocableBytes -= releaseSize;
            if (parent != null) {
                parent.freeRevocableMemory(releaseSize);
            }
            notifyBlockedQuery();
        }
    }

    protected synchronized void freeMemory() {
        freeReserveMemory(reservedBytes);
        freeRevocableMemory(revocableBytes);
    }

    /**
     * Release all associated memory blocks recursively
     */
    public void clear() {
        for (MemoryPool childPool : children.values()) {
            childPool.clear();
        }
        freeMemory();
    }

    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            memoryUsageStat.set(this.getMemoryUsage());
            for (MemoryPool childPool : children.values()) {
                childPool.destroy();
            }

            freeMemory();

            if (parent != null) {
                parent.removeChild(this.getName());
            }
        }
    }

    public MemoryPool getChildPool(String name) {
        return children.get(name);
    }

    public MemoryPool removeChild(String childName) {
        return children.remove(childName);
    }

    public MemoryType getMemoryType() {
        return memoryType;
    }

    public Map<String, MemoryPool> getChildren() {
        return Collections.unmodifiableMap(children);
    }

    public int getChildrenSize() {
        return children.size();
    }

    public String getName() {
        return name;
    }

    public boolean isDestoryed() {
        return destroyed.get();
    }

    public long getMemoryUsageStat() {
        long used = this.getMemoryUsage();
        if (used > 0) {
            return used;
        }
        return memoryUsageStat.get();
    }

    public synchronized long getMaxMemoryUsage() {
        return maxMemoryUsage;
    }

    public synchronized long getMemoryUsage() {
        return reservedBytes + revocableBytes;
    }

    public synchronized long getRevocableBytes() {
        return revocableBytes;
    }

    public synchronized long getReservedBytes() {
        return reservedBytes;
    }

    public synchronized long getFreeBytes() {
        return maxLimit - reservedBytes - revocableBytes;
    }

    public String getFullName() {
        return this.fullName;
    }

    public long getMaxLimit() {
        return this.maxLimit;
    }

    public void setMaxLimit(long memoryLimit) {
        this.maxLimit = memoryLimit;
    }

    public final MemoryPool getParent() {
        return parent;
    }

    // --------------------------------- for spill ------------------------------------------------

    public void requestMemoryRevoke() {
        if (parent != null) {
            parent.requestMemoryRevoke();
        }
    }

    protected void notifyBlockedQuery() {
    }

    protected ListenableFuture<?> block(ListenableFuture<?> parent, long size) {
        return parent == null ? NOT_BLOCKED : parent;
    }

    protected void tryBlock(MemoryAllocateFuture allocFuture, long size, boolean reserved) {
        outOfMemory(fullName, getMemoryUsage(), size, maxLimit, reserved);
    }

    protected void outOfMemory(String memoryPool, long usage, long allocating, long limit, Boolean reserved) {
        throw DingoErrUtil.newStdErr(ErrOutOfMemory);
    }

    protected String printDetailInfo(int level) {
        StringBuilder builder = new StringBuilder();
        String self = "Name=" + name + ", reservedBytes=" + reservedBytes +
            ", revocableBytes=" + revocableBytes;
        builder.append(self);
        String blankStr = " ";
        for (int i = 0; i < level; i++) {
            blankStr += " ";
        }
        level++;
        for (MemoryPool child : children.values()) {
            builder.append("\n");
            builder.append(blankStr).append(child.printDetailInfo(level));
        }
        return builder.toString();
    }
}
