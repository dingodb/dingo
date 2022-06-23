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

package io.dingodb.raft.kv.storage;

import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.raft.Node;
import io.dingodb.raft.entity.Task;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.CONTAINS_KEY;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.COUNT;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.DELETE;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.DELETE_LIST;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.DELETE_RANGE;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.GET;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.MULTI_GET;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.PUT;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.PUT_LIST;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.SCAN;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.SYNC;

@Getter
@ToString
@Builder
public class RaftRawKVOperation {

    public enum Op {
        SYNC,
        PUT,
        PUT_LIST,
        DELETE,
        DELETE_LIST,
        DELETE_RANGE,
        GET,
        MULTI_GET,
        ITERATOR,
        SCAN,
        COUNT,
        CONTAINS_KEY,
        SNAPSHOT_SAVE,
        SNAPSHOT_LOAD,
        BACKUP,
        RESTORE,
        ;
    }

    public static final RaftRawKVOperation SYNC_OP = RaftRawKVOperation.builder().op(SYNC).build();

    private byte[] key;   // startKey for range
    private byte[] value;

    private byte[] extKey;   // endKey for range
    private byte[] extValue;

    private Object ext1;
    private Object ext2;

    private final Op op;

    public <R> R ext1() {
        return (R) ext1;
    }

    public <R> R ext2() {
        return (R) ext2;
    }

    public <T> CompletableFuture<T> applyOnNode(Node node) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Task task = new Task();
        task.setData(ByteBuffer.wrap(encode()));
        task.setDone(new RaftClosure<>(future));
        node.apply(task);
        return future;
    }

    public boolean isBatch() {
        return op == PUT_LIST || op == DELETE_LIST;
    }

    public RaftRawKVOperation toBatchOp() {
        switch (op) {
            case PUT:
                return put(Collections.singletonList(new ByteArrayEntry(key, value)));
            case DELETE:
                return delete(Collections.singletonList(key));
            default:
                return this;
        }
    }

    public boolean merge(RaftRawKVOperation operation) {
        if (ext1 instanceof List && op == operation.op) {
            ((List<?>) ext1).addAll(operation.ext1());
            return true;
        }
        return false;
    }

    public byte[] encode() {
        return ProtostuffCodec.write(this);
    }

    public static RaftRawKVOperation decode(ByteBuffer buffer) {
        return ProtostuffCodec.read(buffer);
    }

    public static RaftRawKVOperation sync() {
        return SYNC_OP;
    }

    public static RaftRawKVOperation put(final byte[] key, final byte[] value) {
        return RaftRawKVOperation.builder()
            .key(key)
            .value(value)
            .op(PUT)
            .build();
    }

    public static RaftRawKVOperation put(final List<ByteArrayEntry> entries) {
        return RaftRawKVOperation.builder()
            .ext1(entries)
            .op(PUT_LIST)
            .build();
    }

    public static RaftRawKVOperation delete(final byte[] key) {
        return RaftRawKVOperation.builder()
            .op(DELETE)
            .key(key)
            .build();
    }

    public static RaftRawKVOperation delete(final List<byte[]> keys) {
        return RaftRawKVOperation.builder()
            .ext1(keys)
            .op(DELETE_LIST)
            .build();
    }

    public static RaftRawKVOperation delete(final byte[] startKey, final byte[] endKey) {
        return RaftRawKVOperation.builder()
            .key(startKey)
            .extKey(endKey)
            .op(DELETE_RANGE)
            .build();
    }

    public static RaftRawKVOperation get(final byte[] key) {
        return RaftRawKVOperation.builder()
            .op(GET)
            .key(key)
            .build();
    }

    public static RaftRawKVOperation get(final List<byte[]> keys) {
        return RaftRawKVOperation.builder()
            .ext1(keys)
            .op(MULTI_GET)
            .build();
    }

    public static RaftRawKVOperation count(final byte[] startKey, final byte[] endKey) {
        return RaftRawKVOperation.builder()
            .key(startKey)
            .extKey(endKey)
            .op(COUNT)
            .build();
    }

    public static RaftRawKVOperation containsKey(final byte[] key) {
        return RaftRawKVOperation.builder()
            .op(CONTAINS_KEY)
            .key(key)
            .build();
    }

    public static RaftRawKVOperation scan(final byte[] startKey, final byte[] endKey) {
        return RaftRawKVOperation.builder()
            .key(startKey)
            .extKey(endKey)
            .op(SCAN)
            .build();
    }

    public static RaftRawKVOperation iterator() {
        return RaftRawKVOperation.builder().op(SCAN).build();
    }

    public static RaftRawKVOperation batch(final Object batch, Op op) {
        return RaftRawKVOperation.builder()
            .ext1(batch)
            .op(op)
            .build();
    }

}
