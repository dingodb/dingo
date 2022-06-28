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

package io.dingodb.raft.closure;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Node;
import io.dingodb.raft.Status;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.util.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class ReadIndexClosure implements Closure {
    private static final Logger LOG = LoggerFactory.getLogger(ReadIndexClosure.class);

    private static final long DEFAULT_TIMEOUT = SystemPropertyUtil.getInt("jraft.read-index.timeout", 2 * 1000);

    private static final int PENDING = 0;
    private static final int COMPLETE = 1;
    private static final int TIMEOUT = 2;

    /**
     * Invalid log index -1.
     */
    public static final long INVALID_LOG_INDEX = -1;

    private long index = INVALID_LOG_INDEX;
    private byte[] requestContext;

    private final AtomicInteger state = new AtomicInteger(PENDING);

    public ReadIndexClosure() {
        this(DEFAULT_TIMEOUT);
    }

    /**
     * Create a read-index closure with a timeout parameter.
     *
     * @param timeoutMs timeout millis
     */
    public ReadIndexClosure(long timeoutMs) {
        if (timeoutMs >= 0) {
            // Lazy to init the timer
            Executors.schedule("read-index-timeout-scanner", this::timeout, timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Called when ReadIndex can be executed.
     *
     * @param status the readIndex status.
     * @param index  the committed index when starts readIndex.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     * @see Node#readIndex(byte[], ReadIndexClosure)
     */
    public abstract void run(final Status status, final long index, final byte[] reqCtx);

    /**
     * Set callback result, called by jraft.
     *
     * @param index  the committed index.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     */
    public void setResult(final long index, final byte[] reqCtx) {
        this.index = index;
        this.requestContext = reqCtx;
    }

    /**
     * The committed log index when starts readIndex request. return -1 if fails.
     *
     * @return returns the committed index.  returns -1 if fails.
     */
    public long getIndex() {
        return this.index;
    }

    /**
     * Returns the request context.
     *
     * @return the request context.
     */
    public byte[] getRequestContext() {
        return this.requestContext;
    }

    @Override
    public void run(final Status status) {
        if (!state.compareAndSet(PENDING, COMPLETE)) {
            LOG.warn("A timeout read-index response finally returned: {}.", status);
            return;
        }

        try {
            run(status, this.index, this.requestContext);
        } catch (final Throwable t) {
            LOG.error("Fail to run ReadIndexClosure with status: {}.", status, t);
        }
    }

    private void timeout() {
        if (!state.compareAndSet(PENDING, TIMEOUT)) {
            return;
        }

        final Status status = new Status(RaftError.ETIMEDOUT, "read-index request timeout");
        try {
            this.run(status, INVALID_LOG_INDEX, null);
        } catch (final Throwable t) {
            LOG.error("[Timeout] fail to run ReadIndexClosure with status: {}.", status, t);
        }
    }

}
