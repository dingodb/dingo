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
import io.dingodb.exec.spill.SpillConfig;
import io.dingodb.exec.spill.SpillException;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
import io.dingodb.exec.tuple.TupleKey;
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

    /** Spill support – lazily initialised. */
    private SpillFileManager spillFileManager;
    private final List<SpillFile> spillFiles = new ArrayList<>();
    private final int spillThreshold;

    public AggCache(TupleMapping keyMapping, @NonNull List<Agg> aggList) {
        this.keyMapping = keyMapping;
        this.aggList = aggList;
        this.cache = new ConcurrentHashMap<>();
        this.spillThreshold = SpillConfig.getAggSpillThreshold();
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
        // Spill partially-aggregated groups when the cache grows too large.
        if (cache.size() >= spillThreshold) {
            spillPartialGroups();
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

    @Override
    public Iterator<Object[]> iterator() {
        if (spillFiles.isEmpty()) {
            // Pure in-memory path – original behaviour.
            if (cache.isEmpty() && keyMapping.size() == 0) {
                return Collections.singleton(aggList.stream().map(agg -> agg.getValue(null)).toArray()).iterator();
            }
            return Iterators.transform(
                cache.entrySet().iterator(),
                e -> ArrayUtils.concat(e.getKey().getTuple(), calValue(e.getValue()))
            );
        }
        // Merge path: re-aggregate spilled partial groups + remaining in-memory groups.
        return mergeAndIterate();
    }

    // -------------------------------------------------------------------------
    // Spill helpers
    // -------------------------------------------------------------------------

    /**
     * Serialises the current in-memory partial groups as output rows (keys + agg values)
     * to a spill file, then clears the in-memory map.
     */
    private void spillPartialGroups() {
        List<Object[]> rows = new ArrayList<>(cache.size());
        for (Map.Entry<TupleKey, Object[]> e : cache.entrySet()) {
            rows.add(ArrayUtils.concat(e.getKey().getTuple(), e.getValue()));
        }
        if (spillFileManager == null) {
            spillFileManager = new SpillFileManager();
        }
        SpillFile sf = spillFileManager.createSpillFile("agg");
        try {
            spillFileManager.write(sf, rows);
        } catch (IOException ex) {
            throw new SpillException("Failed to spill aggregate cache", ex);
        }
        spillFiles.add(sf);
        LogUtils.debug(log, "Spilled {} partial groups to {}", rows.size(), sf.getPath());
        cache.clear();
    }

    /**
     * Reads all spill files and re-merges the partial groups with the remaining
     * in-memory groups, returning a final iterator.
     */
    private Iterator<Object[]> mergeAndIterate() {
        // Spill any remaining in-memory groups so we have a uniform spill-file view.
        if (!cache.isEmpty()) {
            spillPartialGroups();
        }
        // Re-aggregate by re-running reduce() over every spilled partial row.
        for (SpillFile sf : spillFiles) {
            try (SpillFileManager.SpillIterator it = spillFileManager.readIterator(sf)) {
                while (it.hasNext()) {
                    reduce(it.next());
                }
            } catch (IOException e) {
                throw new SpillException("Failed to read spill file " + sf.getPath(), e);
            }
            spillFileManager.delete(sf);
        }
        spillFiles.clear();
        // Now the fully merged result is back in the in-memory cache.
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
        // Clean up any leftover spill files.
        if (spillFileManager != null) {
            for (SpillFile sf : spillFiles) {
                spillFileManager.delete(sf);
            }
        }
        spillFiles.clear();
    }
}
