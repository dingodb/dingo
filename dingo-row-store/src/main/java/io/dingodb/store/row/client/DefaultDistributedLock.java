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

package io.dingodb.store.row.client;

import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import io.dingodb.store.row.errors.InvalidLockAcquirerException;
import io.dingodb.store.row.util.StackTraceUtil;
import io.dingodb.store.row.util.concurrent.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
class DefaultDistributedLock extends DistributedLock<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDistributedLock.class);

    private final DefaultDingoRowStore dingoRowStore;

    private volatile ScheduledFuture<?> watchdogFuture;
    private volatile boolean mayCancelIfRunning = false;

    protected DefaultDistributedLock(byte[] target, long lease, TimeUnit unit, ScheduledExecutorService watchdog,
                                     DefaultDingoRowStore dingoRowStore) {
        super(target, lease, unit, watchdog);
        this.dingoRowStore = dingoRowStore;
    }

    @Override
    public void unlock() {
        final byte[] internalKey = getInternalKey();
        final Acquirer acquirer = getAcquirer();
        try {
            final Owner owner = this.dingoRowStore.releaseLockWith(internalKey, acquirer).get();
            updateOwner(owner);
            if (!owner.isSameAcquirer(acquirer)) {
                final String message = String.format(
                    "an invalid acquirer [%s] trying to unlock, the real owner is [%s]", acquirer, owner);
                throw new InvalidLockAcquirerException(message);
            }
            if (owner.getAcquires() <= 0) {
                tryCancelScheduling();
            }
        } catch (final InvalidLockAcquirerException e) {
            LOG.error("Fail to unlock, {}.", StackTraceUtil.stackTrace(e));
            ThrowUtil.throwException(e);
        } catch (final Throwable t) {
            LOG.error("Fail to unlock: {}, will cancel scheduling, {}.", acquirer, StackTraceUtil.stackTrace(t));
            tryCancelScheduling();
            ThrowUtil.throwException(t);
        }
    }

    @Override
    protected Owner internalTryLock(final byte[] ctx) {
        final byte[] internalKey = getInternalKey();
        final Acquirer acquirer = getAcquirer();
        acquirer.setContext(ctx);
        final CompletableFuture<Owner> future = this.dingoRowStore.tryLockWith(internalKey, false, acquirer);
        final Owner owner = FutureHelper.get(future);
        if (!owner.isSuccess()) {
            updateOwner(owner);
            return owner;
        }

        // if success, update the fencing token in acquirer
        updateOwnerAndAcquirer(owner);

        final ScheduledExecutorService watchdog = getWatchdog();
        if (watchdog == null) {
            return owner;
        }
        // schedule keeping lease
        if (this.watchdogFuture == null) {
            synchronized (this) {
                if (this.watchdogFuture == null) {
                    final long period = (acquirer.getLeaseMillis() / 3) << 1;
                    this.watchdogFuture = scheduleKeepingLease(watchdog, internalKey, acquirer, period);
                }
            }
        }
        return owner;
    }

    private ScheduledFuture<?> scheduleKeepingLease(final ScheduledExecutorService watchdog, final byte[] key,
                                                    final Acquirer acquirer, final long period) {
        return watchdog.scheduleAtFixedRate(() -> {
            try {
                if (this.mayCancelIfRunning) {
                    // last time fail to cancel
                    tryCancelScheduling();
                    return;
                }
                this.dingoRowStore.tryLockWith(key, true, acquirer).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Fail to keeping lease with lock: {}, {}.", acquirer, StackTraceUtil.stackTrace(throwable));
                        tryCancelScheduling();
                        return;
                    }
                    if (!result.isSuccess()) {
                        LOG.warn("Fail to keeping lease with lock: {}, and result detail is: {}.", acquirer, result);
                        tryCancelScheduling();
                        return;
                    }
                    LOG.debug("Keeping lease with lock: {}.", acquirer);
                });
            } catch (final Throwable t) {
                LOG.error("Fail to keeping lease with lock: {}, {}.", acquirer, StackTraceUtil.stackTrace(t));
                tryCancelScheduling();
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    private void tryCancelScheduling() {
        if (this.watchdogFuture != null) {
            this.mayCancelIfRunning = true;
            this.watchdogFuture.cancel(true);
        }
    }
}
