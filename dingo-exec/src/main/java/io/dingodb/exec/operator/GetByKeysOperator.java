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
import com.google.common.collect.Iterators;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.JsonConverter;
import io.dingodb.exec.expr.RtExprWithType;
import io.dingodb.expr.runtime.TupleEvalContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@JsonTypeName("get")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "keys", "selection", "output"})
public final class GetByKeysOperator extends PartIteratorSourceOperator {
    @JsonProperty("keys")
    private final List<Object[]> keyTuples;
    @JsonProperty("filter")
    private final RtExprWithType filter;
    @JsonProperty("selection")
    private final TupleMapping selection;

    @JsonCreator
    public GetByKeysOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("keys") Collection<Object[]> keyTuples,
        @JsonProperty("filter") RtExprWithType filter,
        @JsonProperty("selection") TupleMapping selection
    ) {
        super(tableId, partId, schema, keyMapping);
        // crucial, recover types from json values.
        this.keyTuples = keyTuples.stream()
            .map(i -> (Object[]) schema.select(keyMapping).convertFrom(i, JsonConverter.INSTANCE))
            .collect(Collectors.toList());
        this.filter = filter;
        this.selection = selection;
    }

    @Override
    public void init() {
        super.init();
        Iterator<Object[]> iterator = null;
        if (keyTuples.size() == 1) {
            for (Object[] keyTuple : keyTuples) {
                Object[] tuple = part.getByKey(keyTuple);
                if (tuple != null) {
                    iterator = Iterators.singletonIterator(tuple);
                }
                break;
            }
        } else if (keyTuples.size() != 0) {
            iterator = keyTuples.stream()
                .map(part::getByKey)
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
                .iterator();
        }
        if (iterator != null) {
            if (filter != null) {
                filter.compileIn(schema);
                iterator = Iterators.filter(
                    iterator,
                    tuple -> (boolean) filter.eval(new TupleEvalContext(tuple))
                );
            }
            if (selection != null) {
                this.iterator = Iterators.transform(iterator, selection::revMap);
            } else {
                this.iterator = iterator;
            }
            return;
        }
        this.iterator = Iterators.forArray();
    }
}
