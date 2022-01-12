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

package io.dingodb.store.row.client.failover.impl;

import io.dingodb.store.row.client.FutureGroup;
import io.dingodb.store.row.client.failover.RetryCallable;
import io.dingodb.store.row.errors.ApiExceptionHelper;
import io.dingodb.store.row.util.Attachable;
import io.dingodb.store.row.util.Maps;
import io.dingodb.store.row.util.StackTraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class MapFailoverFuture<K, V> extends CompletableFuture<Map<K, V>> implements Attachable<Object> {
    private static final Logger            LOG = LoggerFactory.getLogger(MapFailoverFuture.class);

    private final int                      retriesLeft;
    private final RetryCallable<Map<K, V>> retryCallable;
    private final Object                   attachments;

    public MapFailoverFuture(int retriesLeft, RetryCallable<Map<K, V>> retryCallable) {
        this(retriesLeft, retryCallable, null);
    }

    public MapFailoverFuture(int retriesLeft, RetryCallable<Map<K, V>> retryCallable, Object attachments) {
        this.retriesLeft = retriesLeft;
        this.retryCallable = retryCallable;
        this.attachments = attachments;
    }

    @Override
    public boolean completeExceptionally(final Throwable ex) {
        if (this.retriesLeft > 0 && ApiExceptionHelper.isInvalidEpoch(ex)) {
            LOG.warn("[InvalidEpoch-Failover] cause: {}, [{}] retries left.", StackTraceUtil.stackTrace(ex),
                    this.retriesLeft);
            final FutureGroup<Map<K, V>> futureGroup = this.retryCallable.run(ex);
            CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
                if (throwable == null) {
                    final Map<K, V> all = Maps.newHashMap();
                    for (final CompletableFuture<Map<K, V>> partOf : futureGroup.futures()) {
                        all.putAll(partOf.join());
                    }
                    super.complete(all);
                } else {
                    super.completeExceptionally(throwable);
                }
            });
            return false;
        }
        if (this.retriesLeft <= 0) {
            LOG.error("[InvalidEpoch-Failover] cause: {}, {} retries left.", StackTraceUtil.stackTrace(ex),
                    this.retriesLeft);
        }
        return super.completeExceptionally(ex);
    }

    @Override
    public Object getAttachments() {
        return attachments;
    }
}
