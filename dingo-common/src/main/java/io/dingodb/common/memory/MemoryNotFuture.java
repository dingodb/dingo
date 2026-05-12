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

import com.google.common.annotations.Beta;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

public class MemoryNotFuture<V> extends AbstractFuture<V> {

    public static <V> MemoryNotFuture<V> create() {
        return new MemoryNotFuture<V>();
    }

    @CanIgnoreReturnValue
    @Override
    public boolean set(@Nullable V value) {
        return super.set(value);
    }

    @CanIgnoreReturnValue
    @Override
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }

    @Beta
    @CanIgnoreReturnValue
    @Override
    public boolean setFuture(ListenableFuture<? extends V> future) {
        return super.setFuture(future);
    }

    private MemoryNotFuture() {}


    @CanIgnoreReturnValue
    @Override
    public final V get() throws InterruptedException, ExecutionException {
        return super.get();
    }

    @CanIgnoreReturnValue
    @Override
    public final V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return super.get(timeout, unit);
    }

    @Override
    public final boolean isDone() {
        return super.isDone();
    }

    @Override
    public final boolean isCancelled() {
        return super.isCancelled();
    }

    @Override
    public final void addListener(Runnable listener, Executor executor) {
        super.addListener(listener, executor);
    }

    @CanIgnoreReturnValue
    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

}
