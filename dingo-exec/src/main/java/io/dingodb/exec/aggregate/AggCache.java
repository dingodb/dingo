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
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.util.Utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

public class AggCache implements Iterable<Object[]> {
    private final TupleMapping keyMapping;
    private final List<Agg> aggList;
    private final Map<AggCacheKey, Object[]> cache;

    public AggCache(TupleMapping keyMapping, @Nonnull List<Agg> aggList) {
        this.keyMapping = keyMapping;
        this.aggList = aggList;
        this.cache = new ConcurrentHashMap<>();
    }

    @Nonnull
    private Object[] getVars(AggCacheKey key) {
        Object[] vars = cache.get(key);
        if (vars == null) {
            vars = new Object[aggList.size()];
            for (int i = 0; i < vars.length; ++i) {
                vars[i] = aggList.get(i).init();
            }
            cache.put(key, vars);
        }
        return vars;
    }

    public void addTuple(Object[] tuple) {
        Object[] keyTuple = keyMapping.revMap(tuple);
        Object[] vars = getVars(new AggCacheKey(keyTuple));
        for (int i = 0; i < vars.length; ++i) {
            vars[i] = aggList.get(i).add(vars[i], tuple);
        }
    }

    public void reduce(Object[] tuple) {
        // Here the keys are leading elements in the tuple.
        int length = keyMapping.size();
        Object[] keyTuple = Arrays.copyOf(tuple, length);
        Object[] vars = getVars(new AggCacheKey(keyTuple));
        for (int i = 0; i < vars.length; ++i) {
            vars[i] = aggList.get(i).merge(vars[i], tuple[length + i]);
        }
    }

    @Override
    public Iterator<Object[]> iterator() {
        return Iterators.transform(
            cache.entrySet().iterator(),
            e -> Utils.combine(e.getKey().getTuple(), e.getValue())
        );
    }
}
