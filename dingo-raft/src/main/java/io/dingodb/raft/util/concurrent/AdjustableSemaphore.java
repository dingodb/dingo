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

package io.dingodb.raft.util.concurrent;

import io.dingodb.raft.util.Requires;

import java.io.Serializable;
import java.util.concurrent.Semaphore;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class AdjustableSemaphore implements Serializable {
    private static final long serialVersionUID = -266635933115069924L;

    private final ResizeableSemaphore semaphore = new ResizeableSemaphore(0);
    private volatile int maxPermits = 0;

    public AdjustableSemaphore() {
    }

    public AdjustableSemaphore(int maxPermits) {
        Requires.requireTrue(maxPermits >= 0, "maxPermits must be a non-negative value");
        setMaxPermits(maxPermits);
    }

    public int getMaxPermits() {
        return maxPermits;
    }

    /**
     * Adjusts the maximum number of available permits.
     *
     * @param newMaxPermits max number of permits
     */
    public synchronized void setMaxPermits(final int newMaxPermits) {
        Requires.requireTrue(newMaxPermits >= 0, "Semaphore permits must be at least 0, but was " + newMaxPermits);

        final int delta = newMaxPermits - this.maxPermits;

        if (delta == 0) {
            return;
        } else if (delta > 0) {
            this.semaphore.release(delta);
        } else {
            this.semaphore.reducePermits(-delta);
        }

        this.maxPermits = newMaxPermits;
    }

    /**
     * Releases a permit, returning it to the semaphore.
     */
    public void release() {
        this.semaphore.release();
    }

    /**
     * Acquires a permit from this semaphore, blocking until one is
     * available, or the thread is {@linkplain Thread#interrupt interrupted}.
     *
     * @throws InterruptedException if the current thread is interrupted
     */
    public void acquire() throws InterruptedException {
        this.semaphore.acquire();
    }

    /**
     * Acquires a permit from this semaphore, only if one is available at the
     * time of invocation.
     *
     * @return {@code true} if a permit was acquired and {@code false}
     * otherwise
     */
    public boolean tryAcquire() {
        return this.semaphore.tryAcquire();
    }

    /**
     * Returns the current number of permits available in this semaphore.
     *
     * @return the number of permits available in this semaphore
     */
    public int availablePermits() {
        return this.semaphore.availablePermits();
    }

    /**
     * Returns if the permits is available of the semaphore.
     *
     * @return {@code true} if current number of permits > 0
     */
    public boolean isAvailable() {
        return availablePermits() > 0;
    }

    private static final class ResizeableSemaphore extends Semaphore {

        private static final long serialVersionUID = 1204115455517785966L;

        public ResizeableSemaphore(int permits) {
            super(permits);
        }

        @Override
        protected void reducePermits(final int reduction) {
            super.reducePermits(reduction);
        }
    }
}
