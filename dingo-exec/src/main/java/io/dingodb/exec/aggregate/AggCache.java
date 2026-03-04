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

package io.dingodb.exec.aggregate;

import com.google.common.collect.Iterators;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ArrayUtils;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
import io.dingodb.exec.spill.TupleSerializer;
import io.dingodb.exec.tuple.TupleKey;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class AggCache implements Iterable<Object[]> {
    private final TupleMapping keyMapping;
    private final List<Agg> aggList;
    private final Map<TupleKey, Object[]> cache;

    // Spill support
    /** Number of in-memory groups that triggers a spill. Disabled (MAX_VALUE) by default. */
    @Setter
    private long spillThreshold = Long.MAX_VALUE;
    private final List<SpillFile> spillFiles = new ArrayList<>();
    private int spillBucketCounter = 0;

    public AggCache(TupleMapping keyMapping, @NonNull List<Agg> aggList) {
        this.keyMapping = keyMapping;
        this.aggList = aggList;
        this.cache = new ConcurrentHashMap<>();
        LogUtils.info(log, "aggCache init");
    }

    private Object @NonNull [] getVars(TupleKey key) {
        return cache.computeIfAbsent(key, k -> new Object[aggList.size()]);
    }

    public void addTuple(Object[] tuple) {
        Object[] keyTuple = keyMapping.revMap(tuple);
        Object[] vars = getVars(new TupleKey(keyTuple));
        for (int i = 0; i < vars.length; ++i) {
            Agg agg = aggList.get(i);
            if (vars[i] == null) {
                vars[i] = agg.first(tuple);
            } else {
                vars[i] = agg.add(vars[i], tuple);
            }
        }
        // Spill partial aggregations when the map exceeds the threshold.
        if (cache.size() > spillThreshold) {
            maybeSpill("agg-cache");
        }
    }

    public void reduce(Object[] tuple) {
        // Here the keys are leading elements in the tuple.
        int length = keyMapping.size();
        Object[] keyTuple = Arrays.copyOf(tuple, length);
        Object[] vars = getVars(new TupleKey(keyTuple));
        for (int i = 0; i < vars.length; ++i) {
            vars[i] = aggList.get(i).merge(vars[i], tuple[length + i]);
        }
    }

    private Object @NonNull [] calValue(Object @NonNull [] vars) {
        Object[] result = new Object[vars.length];
        for (int i = 0; i < vars.length; ++i) {
            result[i] = aggList.get(i).getValue(vars[i]);
        }
        return result;
    }

    /**
     * Spills all current partial aggregation results to disk and clears the in-memory map.
     * Each spilled tuple contains the group key followed by the aggregation intermediate values,
     * allowing them to be merged back via {@link #reduce}.
     *
     * @param operatorId logical operator identifier for the spill file path
     */
    private void maybeSpill(String operatorId) {
        if (cache.isEmpty()) {
            return;
        }
        try {
            SpillFileManager mgr = SpillFileManager.getInstance();
            SpillFile sf = mgr.createSpill(operatorId, spillBucketCounter++);
            // Serialise each entry as key[] + vars[] concatenated into a flat tuple.
            List<Object[]> flatTuples = new ArrayList<>(cache.size());
            for (Map.Entry<TupleKey, Object[]> entry : cache.entrySet()) {
                flatTuples.add(ArrayUtils.concat(entry.getKey().getTuple(), entry.getValue()));
            }
            mgr.write(sf, TupleSerializer.serialize(flatTuples));
            mgr.closeAndFlush(sf);
            spillFiles.add(sf);
            LogUtils.info(log, "aggCache: spilled {} groups to {}", cache.size(), sf.getFile());
            cache.clear();
        } catch (IOException e) {
            LogUtils.warn(log, "aggCache: spill failed, continuing in-memory: {}", e.getMessage());
        }
    }

    @Override
    public Iterator<Object[]> iterator() {
        // Merge spilled partial aggregations back before returning the final iterator.
        if (!spillFiles.isEmpty()) {
            SpillFileManager mgr = SpillFileManager.getInstance();
            for (SpillFile sf : spillFiles) {
                try {
                    byte[] data = mgr.readAllBytes(sf);
                    if (data.length > 0) {
                        for (Object[] flatTuple : TupleSerializer.deserialize(data)) {
                            reduce(flatTuple);
                        }
                    }
                    mgr.release(sf);
                } catch (IOException e) {
                    LogUtils.warn(log, "aggCache: failed to read spill file {}: {}", sf.getFile(), e.getMessage());
                }
            }
            spillFiles.clear();
        }

        if (cache.isEmpty() && keyMapping.size() == 0) {
            return Collections.singleton(aggList.stream().map(agg -> agg.getValue(null)).toArray()).iterator();
        }
        return Iterators.transform(
            cache.entrySet().iterator(),
            e -> ArrayUtils.concat(e.getKey().getTuple(), calValue(e.getValue()))
        );
    }

    public void clear() {
        LogUtils.info(log, "aggCache clear, size:{}", cache.size());
        cache.clear();
        // Release any un-consumed spill files.
        if (!spillFiles.isEmpty()) {
            SpillFileManager mgr = SpillFileManager.getInstance();
            for (SpillFile sf : spillFiles) {
                mgr.release(sf);
            }
            spillFiles.clear();
        }
    }
}
