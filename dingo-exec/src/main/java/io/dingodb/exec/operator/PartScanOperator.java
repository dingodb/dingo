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
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.expr.RtExprWithType;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;

@Slf4j
@JsonTypeName("scan")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "filter", "selection", "output"})
public final class PartScanOperator extends PartIteratorSourceOperator {
    @JsonProperty("filter")
    private final RtExprWithType filter;
    @JsonProperty("selection")
    private final TupleMapping selection;

    @JsonCreator
    public PartScanOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") TupleSchema schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") RtExprWithType filter,
        @JsonProperty("selection") TupleMapping selection
    ) {
        super(tableId, partId, schema, keyMapping);
        this.filter = filter;
        this.selection = selection;
    }

    @Override
    public void init() {
        final long startTime = System.currentTimeMillis();
        super.init();
        Iterator<Object[]> iterator = part.getIterator();
        if (filter != null) {
            filter.compileIn(schema);
            iterator = Iterators.filter(
                iterator,
                tuple -> RtLogicalOp.test(filter.eval(new TupleEvalContext(tuple)))
            );
        }
        if (selection != null) {
            iterator = Iterators.transform(iterator, selection::revMap);
        }
        this.iterator = iterator;
        if (log.isDebugEnabled()) {
            log.debug("PartScanOperator init, cost: {}ms.", System.currentTimeMillis() - startTime);
        }
    }
}
