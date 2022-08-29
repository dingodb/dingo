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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public final class PhaseAck<T> {

    CompletableFuture<Long> clock = new CompletableFuture<>();
    CompletableFuture<T> result;

    PhaseAck() {
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

    public T join() {
        clock.join();
        return result.join();
    }

    public T get() throws ExecutionException, InterruptedException {
        clock.join();
        return result.get();
    }

    public T get(long ttl, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        clock.join();
        return result.get(ttl, unit);
    }

    public void whenClockCompleteAsync(BiConsumer<Long, Throwable> consumer, Executor executor) {
        clock.whenCompleteAsync(consumer, executor);
    }

    public void whenCompleteAsync(BiConsumer<T, Throwable> consumer, Executor executor) {
        clock.join();
        result.whenCompleteAsync(consumer, executor);
    }

}
