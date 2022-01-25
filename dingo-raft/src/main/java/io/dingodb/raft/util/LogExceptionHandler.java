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

package io.dingodb.raft.util;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class LogExceptionHandler<T> implements ExceptionHandler<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogExceptionHandler.class);

    public interface OnEventException<T> {
        void onException(T event, Throwable ex);
    }

    private final String name;
    private final OnEventException<T> onEventException;

    public LogExceptionHandler(String name) {
        this(name, null);
    }

    public LogExceptionHandler(String name, OnEventException<T> onEventException) {
        this.name = name;
        this.onEventException = onEventException;
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        LOG.error("Fail to start {} disruptor", this.name, ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        LOG.error("Fail to shutdown {} disruptor", this.name, ex);
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        LOG.error("Handle {} disruptor event error, event is {}", this.name, event, ex);
        if (this.onEventException != null) {
            this.onEventException.onException(event, ex);
        }
    }
}
