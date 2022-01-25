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

package io.dingodb.server.coordinator.store;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.util.NoBreakFunctionWrapper;
import io.dingodb.common.util.PreParameters;
import io.dingodb.common.util.StackTraces;
import io.dingodb.raft.Node;
import io.dingodb.store.row.client.failover.impl.FailoverClosureImpl;
import io.dingodb.store.row.storage.KVEntry;
import io.dingodb.store.row.storage.KVStoreClosure;
import io.dingodb.store.row.storage.RaftRawKVStore;
import io.dingodb.store.row.storage.RawKVStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

@Slf4j
public class RaftAsyncKeyValueStore implements AsyncKeyValueStore {

    private final Map<byte[], List<WatchKeyHandler>> readHandlers = new HashMap<>();
    private final Map<byte[], List<WatchKeyHandler>> writeHandlers = new HashMap<>();
    private final Map<byte[], List<WatchKeyHandler>> deleteHandlers = new HashMap<>();
    private final Map<byte[], List<WatchKeyHandler>> touchHandlers = new HashMap<>();

    private final List<WatchKeyHandler> readAnyHandlers = new ArrayList<>();
    private final List<WatchKeyHandler> writeAnyHandlers = new ArrayList<>();
    private final List<WatchKeyHandler> deleteAnyHandlers = new ArrayList<>();
    private final List<WatchKeyHandler> touchAnyHandlers = new ArrayList<>();

    private final ExecutorService watchExecutor;
    private final ExecutorService multiKVOpExecutor;

    private final Node node;

    private final RawKVStore rawKVStore;
    private RawKVStore raftRawKVStore;

    private boolean watchWithStack = false;

    public RaftAsyncKeyValueStore(RawKVStore rawKVStore, Node node) {
        this.watchExecutor = watchExecutor();
        this.multiKVOpExecutor = multiKVOpExecutor();
        this.rawKVStore = rawKVStore;
        this.node = node;
    }

    @Override
    public void init() {
        raftRawKVStore = new RaftRawKVStore(this.node, rawKVStore, readIndexExecutor());
        log.info("Create raft async key value store finish.");
    }

    public RaftAsyncKeyValueStore watchWithStack(boolean watchWithStack) {
        this.watchWithStack = watchWithStack;
        return this;
    }

    private ThreadPoolExecutor multiKVOpExecutor() {
        return new ThreadPoolBuilder()
            .name("Async-kv-store")
            .build();
    }

    private ThreadPoolExecutor watchExecutor() {
        return new ThreadPoolBuilder()
            .name("Async-kv-store-watch")
            .build();
    }

    private ThreadPoolExecutor readIndexExecutor() {
        return new ThreadPoolBuilder()
            .name("Raft-read-index")
            .build();
    }

    @Override
    public synchronized void watchKeyRead(byte[] key, WatchKeyHandler handler) {
        if (key == null || key.length == 0) {
            readAnyHandlers.add(handler);
            return;
        }
        PreParameters.cleanNull(readHandlers.get(key), ArrayList<WatchKeyHandler>::new).add(handler);
    }

    @Override
    public synchronized void watchKeyWrite(byte[] key, WatchKeyHandler handler) {
        if (key == null || key.length == 0) {
            writeAnyHandlers.add(handler);
            return;
        }
        PreParameters.cleanNull(writeHandlers.get(key), ArrayList<WatchKeyHandler>::new).add(handler);
    }

    @Override
    public void watchKeyDelete(byte[] key, WatchKeyHandler handler) {
        if (key == null || key.length == 0) {
            deleteAnyHandlers.add(handler);
            return;
        }
        PreParameters.cleanNull(deleteHandlers.get(key), ArrayList<WatchKeyHandler>::new).add(handler);
    }

    @Override
    public void watchKeyTouch(byte[] key, WatchKeyHandler handler) {
        if (key == null || key.length == 0) {
            touchAnyHandlers.add(handler);
            return;
        }
        PreParameters.cleanNull(touchHandlers.get(key), ArrayList<WatchKeyHandler>::new).add(handler);
    }

    private void onTouch(byte[] key, String stack) {
        PreParameters
            .cleanNull(touchHandlers.get(key), Collections.<WatchKeyHandler>emptyList())
            .forEach(NoBreakFunctionWrapper.wrap(handler -> {
                handler.onTouch(key, stack);
            }));
        touchAnyHandlers.forEach(NoBreakFunctionWrapper.wrap(handler -> {
            handler.onTouch(key, stack);
        }));
    }

    private void onRead(byte[] key, byte[] value, String stack) {
        PreParameters
            .cleanNull(readHandlers.get(key), Collections.<WatchKeyHandler>emptyList())
            .forEach(NoBreakFunctionWrapper.wrap(handler -> {
                handler.onRead(key, value, stack);
            }));
        readAnyHandlers.forEach(NoBreakFunctionWrapper.wrap(handler -> {
            handler.onRead(key, value, stack);
        }));
    }

    private void onWrite(byte[] key, byte[] value, String stack) {
        PreParameters
            .cleanNull(writeHandlers.get(key), Collections.<WatchKeyHandler>emptyList())
            .forEach(NoBreakFunctionWrapper.wrap(handler -> {
                handler.onWrite(key, value, stack);
            }));
        writeAnyHandlers.forEach(NoBreakFunctionWrapper.wrap(handler -> {
            handler.onWrite(key, value, stack);
        }));
    }

    private void onDelete(byte[] key, String stack) {
        PreParameters
            .cleanNull(deleteHandlers.get(key), Collections.<WatchKeyHandler>emptyList())
            .forEach(NoBreakFunctionWrapper.wrap(handler -> {
                handler.onDelete(key, stack);
            }));
        deleteAnyHandlers.forEach(NoBreakFunctionWrapper.wrap(handler -> {
            handler.onDelete(key, stack);
        }));
    }

    @Override
    public CompletableFuture<Boolean> contains(byte[] key) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<Boolean> future = doKVOp(closure -> rawKVStore.containsKey(key, closure));
        future.thenAcceptAsync(b -> onTouch(key, stack), watchExecutor);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> put(byte[] key, byte[] value) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<Boolean> future = doKVOp(closure -> raftRawKVStore.put(key, value, closure));
        future.thenAcceptAsync(b -> onWrite(key, value, stack), watchExecutor);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> delete(byte[] key) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<Boolean> future = doKVOp(closure -> raftRawKVStore.delete(key, closure));
        future.thenAcceptAsync(b -> onDelete(key, stack), watchExecutor);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<byte[]> future = doKVOp(closure -> rawKVStore.get(key, closure));
        future.thenAcceptAsync(value -> onRead(key, value, stack), watchExecutor);
        return future;
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(byte[] start, byte[] end) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<List<KVEntry>> future = doKVOp(closure -> rawKVStore.scan(start, end, closure));
        future.thenAcceptAsync(value -> value.forEach(e -> onRead(e.getKey(), e.getValue(), stack)), watchExecutor);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(byte[] key, byte[] value) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<byte[]> future = doKVOp(closure -> raftRawKVStore.getAndPut(key, value, closure));
        future.thenAcceptAsync(oldV -> onWrite(key, value, stack), watchExecutor);
        future.thenAcceptAsync(oldV -> onRead(key, oldV, stack), watchExecutor);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> merge(byte[] key, byte[] value) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> future = doKVOp(closure -> raftRawKVStore.merge(key, value, closure));
        future.thenAcceptAsync(b -> this.<byte[]>doKVOp(closure -> rawKVStore.get(key, closure))
            .thenAcceptAsync(r -> {
                resultFuture.complete(r);
                onWrite(key, r, stack);
            }, multiKVOpExecutor), watchExecutor);
        return resultFuture;
    }

    @Override
    public CompletableFuture<Long> increment(byte[] key) {
        String stack = watchWithStack ? StackTraces.stack(2) : null;
        CompletableFuture<Long> future = new CompletableFuture<>();
        get(key).whenCompleteAsync((r, e) -> {
            if (e == null) {
                if (r == null) {
                    put(key, PrimitiveCodec.encodeVarLong(1)).whenCompleteAsync((r1, e1) -> {
                        if (e1 == null) {
                            future.complete(1L);
                        } else {
                            future.completeExceptionally(e1);
                        }
                    }, multiKVOpExecutor);
                } else {
                    long value = PrimitiveCodec.readVarLong(r) + 1;
                    put(key, PrimitiveCodec.encodeVarLong(value)).whenCompleteAsync((r1, e1) -> {
                        if (e1 == null) {
                            future.complete(value);
                        } else {
                            future.completeExceptionally(e1);
                        }
                    }, multiKVOpExecutor);
                }
            } else {
                future.completeExceptionally(e);
            }
        }, multiKVOpExecutor);
        future.thenAcceptAsync(v -> onWrite(key, PrimitiveCodec.encodeVarLong(v), stack), watchExecutor);
        future.join();
        return future;
    }

    protected <T> CompletableFuture<T> doKVOp(Consumer<KVStoreClosure> opFunc) {
        CompletableFuture<T> future = new CompletableFuture<>();
        doKVOp(opFunc, 3, future);
        return future;
    }

    private <T> void doKVOp(Consumer<KVStoreClosure> opFunc, int retries, CompletableFuture<T> future) {
        FailoverClosureImpl<T> closure = new FailoverClosureImpl<>(
            future,
            retries,
            err -> doKVOp(opFunc, retries - 1, future)
        );
        opFunc.accept(closure);
    }
}
