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

package io.dingodb.mpu.core;

import io.dingodb.common.util.Optional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public final class PhaseAck {

    CompletableFuture<Long> clock = new CompletableFuture<>();
    CompletableFuture<Object> result = new CompletableFuture<>();

    PhaseAck() {
        clock.whenComplete((r, e) -> Optional.ifPresent(e, this::completeExceptionally));
    }

    protected void completeClock(long clock) {
        this.clock.complete(clock);
    }

    protected void completeResult(Object result) {
        this.result.complete(result);
    }

    protected void completeExceptionally(Throwable throwable) {
        this.result.completeExceptionally(throwable);
    }

    public long joinClock() {
        return clock.join();
    }

    public long getClock() throws ExecutionException, InterruptedException {
        return clock.get();
    }

    public long getClock(long ttl, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        return clock.get(ttl, unit);
    }

    public <V> V join() {
        joinClock();
        return (V) result.join();
    }

    public <V> V get() throws ExecutionException, InterruptedException {
        joinClock();
        return (V) result.get();
    }

    public <V> V get(long ttl, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        joinClock();
        return (V) result.get(ttl, unit);
    }

    public void whenClockCompleteAsync(BiConsumer<Long, Throwable> consumer, Executor executor) {
        clock.whenCompleteAsync(consumer, executor);
    }

    public <V> void whenCompleteAsync(BiConsumer<V, Throwable> consumer, Executor executor) {
        joinClock();
        result.whenCompleteAsync((r, e) -> {
            consumer.accept((V) r, e);
        }, executor);
    }

}
