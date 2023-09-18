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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.tuple.TupleKey;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class IndexMergeOperator extends SoleOutOperator {
    @JsonProperty("keys")
    private final TupleMapping keyMapping;

    @JsonProperty
    private final TupleMapping selection;

    private ConcurrentHashMap<TupleKey, Object[]> hashMap;

    public IndexMergeOperator(TupleMapping keyMapping, TupleMapping selection) {
        this.keyMapping = keyMapping;
        this.selection = transformSelection(selection);
    }

    @Override
    public void init() {
        super.init();
        hashMap = new ConcurrentHashMap<>();
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple) {
        Object[] keyTuple = keyMapping.revMap(tuple);
        hashMap.put(new TupleKey(keyTuple), selection.revMap(tuple));
        return true;
    }


    @Override
    public void fin(int pin, @Nullable Fin fin) {
        hashMap.values().forEach(objects -> output.push(objects));
        output.fin(fin);
        // Reset
        hashMap.clear();
    }

    private TupleMapping transformSelection(TupleMapping selection) {
        List<Integer> mappings = new ArrayList();
        for (int i = 0; i < selection.size(); i ++) {
            mappings.add(i);
        }

        return TupleMapping.of(mappings);
    }

}
