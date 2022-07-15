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
import io.dingodb.exec.expr.RtExprWithType;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;

@Slf4j
@JsonTypeName("rangeScan")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "filter", "selection", "output", "startKey", "endKey",
    "includeStart", "includeEnd"})
public final class PartRangeScanOperator extends PartIteratorSourceOperator {
    @JsonProperty("filter")
    private final RtExprWithType filter;
    @JsonProperty("selection")
    private final TupleMapping selection;
    @JsonProperty("startKey")
    private final byte[] startKey;
    @JsonProperty("endKey")
    private final byte[] endKey;
    @JsonProperty("includeStart")
    private final boolean includeStart;
    @JsonProperty("includeEnd")
    private final boolean includeEnd;

    @JsonCreator
    public PartRangeScanOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") RtExprWithType filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("endKey") byte[] endKey,
        @JsonProperty("includeStart") boolean includeStart,
        @JsonProperty("includeEnd") boolean includeEnd
    ) {
        super(tableId, partId, schema, keyMapping);
        this.filter = filter;
        this.selection = selection;
        this.startKey = startKey;
        this.endKey = endKey;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
    }

    @Override
    public void init() {
        final long startTime = System.currentTimeMillis();
        super.init();
        Iterator<Object[]> iterator = part.getIteratorByRange(startKey, endKey, includeStart, includeEnd);
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
            log.debug("PartRangeScanOperator init, cost: {}ms.", System.currentTimeMillis() - startTime);
        }
    }
}
