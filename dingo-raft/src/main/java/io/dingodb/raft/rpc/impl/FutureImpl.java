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

package io.dingodb.raft.rpc.impl;

import io.dingodb.net.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class FutureImpl<R> implements Future<R> {

    private static final Logger LOG = LoggerFactory.getLogger(FutureImpl.class);

    protected final ReentrantLock lock;

    protected boolean isDone;

    protected CountDownLatch latch;

    protected boolean isCancelled;
    protected Throwable failure;

    protected Channel channel = null;

    protected R result;

    public FutureImpl() {
        this(null, new ReentrantLock());
    }

    public FutureImpl(Channel channel) {
        this(channel, new ReentrantLock());
    }

    public FutureImpl(Channel channel, ReentrantLock lock) {
        this.channel = channel;
        this.lock = lock;
        this.latch = new CountDownLatch(1);
    }

    /**
     * Get current result value without any blocking.
     *
     * @return current result value without any blocking.
     */
    public R getResult() {
        this.lock.lock();
        try {
            return this.result;
        } finally {
            this.lock.unlock();
        }
    }

    public Throwable getFailure() {
        this.lock.lock();
        try {
            return this.failure;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Set the result value and notify about operation completion.
     *
     * @param result
     *            the result value
     */
    public void setResult(R result) {
        this.lock.lock();
        try {
            this.result = result;
            notifyHaveResult();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        this.lock.lock();
        try {
            this.isCancelled = true;
            notifyHaveResult();
            return true;
        } finally {
            this.lock.unlock();
            closeChannel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCancelled() {
        try {
            this.lock.lock();
            return this.isCancelled;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        this.lock.lock();
        try {
            return this.isDone;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public R get() throws InterruptedException, ExecutionException {
        this.latch.await();
        this.lock.lock();
        try {
            if (this.isCancelled) {
                throw new CancellationException();
            } else if (this.failure != null) {
                throw new ExecutionException(this.failure);
            }

            return this.result;
        } finally {
            this.lock.unlock();
            closeChannel();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public R get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException,
                                                         TimeoutException {
        final boolean isTimeOut = !latch.await(timeout, unit);
        this.lock.lock();
        try {
            if (!isTimeOut) {
                if (this.isCancelled) {
                    throw new CancellationException();
                } else if (this.failure != null) {
                    throw new ExecutionException(this.failure);
                }

                return this.result;
            } else {
                throw new TimeoutException();
            }
        } finally {
            this.lock.unlock();
            closeChannel();
        }
    }

    /**
     * Notify about the failure, occured during asynchronous operation
     * execution.
     */
    public void failure(final Throwable failure) {
        this.lock.lock();
        try {
            this.failure = failure;
            notifyHaveResult();
            closeChannel();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Notify blocked listeners threads about operation completion.
     */
    protected void notifyHaveResult() {
        this.isDone = true;
        this.latch.countDown();
    }

    private void closeChannel() {
        if (this.channel != null) {
            try {
                this.channel.close();
            } catch (Exception e) {
                LOG.error("Fail to close channel : id-{}, {}", channel.channelId(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
