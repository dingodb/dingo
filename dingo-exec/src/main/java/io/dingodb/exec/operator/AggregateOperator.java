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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.exec.aggregate.AbstractAgg;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.AggCache;
import io.dingodb.exec.fin.Fin;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@JsonTypeName("aggregate")
@JsonPropertyOrder({"keys", "aggregates", "output"})
public final class AggregateOperator extends SoleOutOperator {
    @JsonProperty("keys")
    private final TupleMapping keyMapping;
    @JsonProperty("aggregates")
    @JsonSerialize(contentAs = AbstractAgg.class)
    @JsonDeserialize(contentAs = AbstractAgg.class)
    private final List<Agg> aggList;
    private AggCache cache;

    @JsonCreator
    public AggregateOperator(
        @JsonProperty("keys") TupleMapping keyMapping,
        @JsonProperty("aggregates") List<Agg> aggList
    ) {
        super();
        this.keyMapping = keyMapping;
        this.aggList = aggList;
    }

    @Override
    public void init() {
        super.init();
        cache = new AggCache(keyMapping, aggList);
    }

    @Override
    public synchronized boolean push(int pin, Object[] tuple) {
        cache.addTuple(tuple);
        return true;
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        for (Object[] t : cache) {
            if (!output.push(t)) {
                break;
            }
        }
        output.fin(fin);
    }
}
