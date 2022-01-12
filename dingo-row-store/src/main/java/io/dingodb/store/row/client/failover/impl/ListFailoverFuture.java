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
import io.dingodb.store.row.client.failover.ListRetryCallable;
import io.dingodb.store.row.errors.ApiExceptionHelper;
import io.dingodb.store.row.util.Attachable;
import io.dingodb.store.row.util.Lists;
import io.dingodb.store.row.util.StackTraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class ListFailoverFuture<T> extends CompletableFuture<List<T>> implements Attachable<Object> {
    private static final Logger        LOG = LoggerFactory.getLogger(ListFailoverFuture.class);

    private final int                  retriesLeft;
    private final ListRetryCallable<T> retryCallable;
    private final Object               attachments;

    public ListFailoverFuture(int retriesLeft, ListRetryCallable<T> retryCallable) {
        this(retriesLeft, retryCallable, null);
    }

    public ListFailoverFuture(int retriesLeft, ListRetryCallable<T> retryCallable, Object attachments) {
        this.retriesLeft = retriesLeft;
        this.retryCallable = retryCallable;
        this.attachments = attachments;
    }

    @Override
    public boolean completeExceptionally(final Throwable ex) {
        if (this.retriesLeft > 0 && ApiExceptionHelper.isInvalidEpoch(ex)) {
            LOG.warn("[InvalidEpoch-Failover] cause: {}, [{}] retries left.", StackTraceUtil.stackTrace(ex),
                    this.retriesLeft);
            final FutureGroup<List<T>> futureGroup = this.retryCallable.run(ex);
            CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
                if (throwable == null) {
                    final List<T> all = Lists.newArrayList();
                    for (final CompletableFuture<List<T>> partOf : futureGroup.futures()) {
                        all.addAll(partOf.join());
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
