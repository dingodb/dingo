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

package io.dingodb.store.row.storage;

import com.codahale.metrics.Timer;
import io.dingodb.store.row.metrics.KVMetrics;
import io.dingodb.store.row.util.concurrent.DistributedLock;

import java.util.List;

import static io.dingodb.store.row.metrics.KVMetricNames.RPC_REQUEST_HANDLE_TIMER;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class MetricsRawKVStore implements RawKVStore {
    private final String regionId;
    private final RawKVStore rawKVStore;
    private final Timer timer;

    public MetricsRawKVStore(String regionId, RawKVStore rawKVStore) {
        this.regionId = regionId;
        this.rawKVStore = rawKVStore;
        this.timer = KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId);
    }

    @Override
    public KVIterator localIterator() {
        return this.rawKVStore.localIterator();
    }

    @Override
    public void get(final byte[] key, final KVStoreClosure closure) {
        get(key, true, closure);
    }

    @Override
    public void get(final byte[] key, final boolean readOnlySafe, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.GET, 1, 0);
        this.rawKVStore.get(key, readOnlySafe, c);
    }

    @Override
    public void multiGet(final List<byte[]> keys, final KVStoreClosure closure) {
        multiGet(keys, true, closure);
    }

    @Override
    public void multiGet(final List<byte[]> keys, final boolean readOnlySafe, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.MULTI_GET, keys.size(), 0);
        this.rawKVStore.multiGet(keys, readOnlySafe, c);
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.CONTAINS_KEY, 1, 0);
        this.rawKVStore.containsKey(key, c);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        scan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final boolean returnValue, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.SCAN, 0, 0);
        this.rawKVStore.scan(startKey, endKey, limit, readOnlySafe, returnValue, c);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                            final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                            final boolean returnValue, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                            final KVStoreClosure closure) {
        reverseScan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                            final boolean returnValue, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.REVERSE_SCAN, 0, 0);
        this.rawKVStore.reverseScan(startKey, endKey, limit, readOnlySafe, returnValue, c);
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.GET_SEQUENCE, 1, 8);
        this.rawKVStore.getSequence(seqKey, step, c);
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.RESET_SEQUENCE, 1, 0);
        this.rawKVStore.resetSequence(seqKey, c);
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.PUT, 1, value.length);
        this.rawKVStore.put(key, value, c);
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.GET_PUT, 1, value.length);
        this.rawKVStore.getAndPut(key, value, c);
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.COMPARE_PUT, 1, update.length);
        this.rawKVStore.compareAndPut(key, expect, update, c);
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.MERGE, 1, value.length);
        this.rawKVStore.merge(key, value, c);
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        long bytesWritten = 0;
        for (final KVEntry kvEntry : entries) {
            byte[] value = kvEntry.getValue();
            bytesWritten += (value == null ? 0 : value.length);
        }
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.PUT_LIST, entries.size(), bytesWritten);
        this.rawKVStore.put(entries, c);
    }

    @Override
    public void compareAndPutAll(final List<CASEntry> entries, final KVStoreClosure closure) {
        long bytesWritten = 0;
        for (final CASEntry casEntry : entries) {
            byte[] value = casEntry.getUpdate();
            bytesWritten += (value == null ? 0 : value.length);
        }
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.COMPARE_PUT_ALL, entries.size(), bytesWritten);
        this.rawKVStore.compareAndPutAll(entries, c);
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.PUT_IF_ABSENT, 1, value.length);
        this.rawKVStore.putIfAbsent(key, value, c);
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        // 'keysCount' and 'bytesWritten' can't be provided with exact numbers, but I endured
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.KEY_LOCK, 2, 0);
        this.rawKVStore.tryLockWith(key, fencingKey, keepLease, acquirer, c);
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        // 'keysCount' and 'bytesWritten' can't be provided with exact numbers, but I endured
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.KEY_LOCK_RELEASE, 2, 0);
        this.rawKVStore.releaseLockWith(key, acquirer, c);
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.DELETE, 1, 0);
        this.rawKVStore.delete(key, c);
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.DELETE_RANGE, 0, 0);
        this.rawKVStore.deleteRange(startKey, endKey, c);
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.DELETE_LIST, keys.size(), 0);
        this.rawKVStore.delete(keys, c);
    }

    @Override
    public void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure) {
        final KVStoreClosure c = metricsAdapter(closure, KVOperation.NODE_EXECUTE, 0, 0);
        this.rawKVStore.execute(nodeExecutor, isLeader, c);
    }

    private MetricsKVClosureAdapter metricsAdapter(final KVStoreClosure closure, final byte op, final long keysCount,
                                                   final long bytesWritten) {
        return new MetricsKVClosureAdapter(closure, this.regionId, op, keysCount, bytesWritten, timeCtx());
    }

    private Timer.Context timeCtx() {
        return this.timer.time();
    }
}
