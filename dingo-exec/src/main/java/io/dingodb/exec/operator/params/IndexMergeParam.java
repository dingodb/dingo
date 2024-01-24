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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.tuple.TupleKey;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@JsonTypeName("merge")
@JsonPropertyOrder({"keys", "selection"})
public class IndexMergeParam extends AbstractParams {

    @JsonProperty("keys")
    private final TupleMapping keyMapping;
    @JsonProperty("selection")
    private final TupleMapping selection;
    private transient ConcurrentHashMap<TupleKey, Object[]> hashMap;

    public IndexMergeParam(TupleMapping keyMapping, TupleMapping selection) {
        this.keyMapping = keyMapping;
        this.selection = transformSelection(selection);
    }

    public void init(Vertex vertex) {
        hashMap = new ConcurrentHashMap<>();
    }

    private TupleMapping transformSelection(TupleMapping selection) {
        List<Integer> mappings = new ArrayList<>();
        for (int i = 0; i < selection.size(); i ++) {
            mappings.add(i);
        }

        return TupleMapping.of(mappings);
    }

    public void clear() {
        hashMap.clear();
    }
}
