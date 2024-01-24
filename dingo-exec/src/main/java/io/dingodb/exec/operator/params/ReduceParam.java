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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.aggregate.AbstractAgg;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.AggCache;
import io.dingodb.exec.dag.Vertex;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@Getter
@JsonTypeName("reduce")
@JsonPropertyOrder({"inputNum", "keys", "aggregates"})
public class ReduceParam extends AbstractParams {

    @JsonProperty("keys")
    private final TupleMapping keys;
    @JsonProperty("aggregates")
    @JsonSerialize(contentAs = AbstractAgg.class)
    @JsonDeserialize(contentAs = AbstractAgg.class)
    private final List<Agg> aggList;

    private transient AggCache cache;

    public ReduceParam(TupleMapping keys, List<Agg> aggList) {
        this.keys = keys;
        this.aggList = aggList;
    }

    @Override
    public void init(Vertex vertex) {
        cache = new AggCache(keys, aggList);
    }

    public synchronized void reduce(Object[] tuple) {
        cache.reduce(tuple);
    }
}
