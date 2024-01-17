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
import io.dingodb.exec.dag.Vertex;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@JsonTypeName("partition")
@JsonPropertyOrder({"parentIds", "keyMapping", "partIndices"})
public class PartitionParam extends AbstractParams {
    @JsonProperty("parentIds")
    private final List<Long> parentIds;
    @JsonProperty("tableDefinition")
    private final Table table;
    @Setter
    @JsonProperty("partIndices")
    private Map<Long, Integer> partIndices;

    public PartitionParam(
        Set<Long> parentIds,
        Table table
    ) {
        this.parentIds = new ArrayList<>(parentIds);
        this.table = table;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        partIndices = new HashMap<>();
        for (int i = 0; i < parentIds.size(); i++) {
            partIndices.put(parentIds.get(i), i);
        }
    }
}
