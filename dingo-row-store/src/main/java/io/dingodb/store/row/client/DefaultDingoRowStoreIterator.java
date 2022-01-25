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

package io.dingodb.store.row.client;

import io.dingodb.raft.util.BytesUtil;
import io.dingodb.store.row.client.pd.PlacementDriverClient;
import io.dingodb.store.row.storage.KVEntry;

import java.util.ArrayDeque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class DefaultDingoRowStoreIterator implements DingoRowStoreIterator<KVEntry> {
    private final DefaultDingoRowStore dingoRowStore;
    private final PlacementDriverClient pdClient;
    private final byte[] startKey;
    private final byte[] endKey;
    private final boolean readOnlySafe;
    private final boolean returnValue;
    private final int bufSize;
    private final Queue<KVEntry> buf;

    private byte[] cursorKey;

    public DefaultDingoRowStoreIterator(DefaultDingoRowStore dingoRowStore, byte[] startKey, byte[] endKey, int bufSize,
                                        boolean readOnlySafe, boolean returnValue) {
        this.dingoRowStore = dingoRowStore;
        this.pdClient = dingoRowStore.getPlacementDriverClient();
        this.startKey = BytesUtil.nullToEmpty(startKey);
        this.endKey = endKey;
        this.bufSize = bufSize;
        this.readOnlySafe = readOnlySafe;
        this.returnValue = returnValue;
        this.buf = new ArrayDeque<>(bufSize);
        this.cursorKey = this.startKey;
    }

    @Override
    public synchronized boolean hasNext() {
        if (this.buf.isEmpty()) {
            while (this.endKey == null || BytesUtil.compare(this.cursorKey, this.endKey) < 0) {
                final List<KVEntry> kvEntries = this.dingoRowStore.singleRegionScan(this.cursorKey, this.endKey,
                    this.bufSize, this.readOnlySafe, this.returnValue);
                if (kvEntries.isEmpty()) {
                    // cursorKey jump to next region's startKey
                    this.cursorKey = this.pdClient.findStartKeyOfNextRegion(this.cursorKey, false);
                    if (cursorKey == null) { // current is the last region
                        break;
                    }
                } else {
                    final KVEntry last = kvEntries.get(kvEntries.size() - 1);
                    this.cursorKey = BytesUtil.nextBytes(last.getKey()); // cursorKey++
                    this.buf.addAll(kvEntries);
                    break;
                }
            }
            return !this.buf.isEmpty();
        }
        return true;
    }

    @Override
    public synchronized KVEntry next() {
        if (this.buf.isEmpty()) {
            throw new NoSuchElementException();
        }
        return this.buf.poll();
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public int getBufSize() {
        return bufSize;
    }
}
