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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.zip.Checksum;

public interface RawKVStore {

    default void init() {
    }

    default void close() {
    }

    SeekableIterator<byte[], ByteArrayEntry> iterator();

    byte[] get(byte[] key);

    List<ByteArrayEntry> get(List<byte[]> keys);

    boolean containsKey(byte[] key);

    SeekableIterator<byte[], ByteArrayEntry> scan(byte[] startKey, byte[] endKey);

    void put(byte[] key, byte[] value);

    void put(List<ByteArrayEntry> entries);

    boolean delete(byte[] key);

    boolean delete(List<byte[]> keys);

    boolean delete(byte[] startKey, byte[] endKey);

    CompletableFuture<Checksum> snapshotSave(String path);

    CompletableFuture<Checksum> snapshotSave(String path, byte[] startKey, byte[] endKey);

    CompletableFuture<Boolean> snapshotLoad(String path, String checksum);

    CompletableFuture<Boolean> snapshotLoad(String path, String checksum, byte[] startKey, byte[] endKey);

}
