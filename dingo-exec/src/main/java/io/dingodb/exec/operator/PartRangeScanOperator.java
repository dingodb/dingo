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
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

@Slf4j
@JsonTypeName("rangeScan")
@JsonPropertyOrder({
    "table", "part", "schema", "keyMapping", "filter", "selection", "output",
    "startKey", "endKey", "includeStart", "includeEnd", "prefixScan"
})
public final class PartRangeScanOperator extends PartIteratorSourceOperator {
    @JsonProperty("startKey")
    private final byte[] startKey;
    @JsonProperty("endKey")
    private final byte[] endKey;
    @JsonProperty("includeStart")
    private final boolean includeStart;
    @JsonProperty("includeEnd")
    private final boolean includeEnd;
    @JsonProperty("prefixScan")
    private final boolean prefixScan;

    @JsonCreator
    public PartRangeScanOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("endKey") byte[] endKey,
        @JsonProperty("includeStart") boolean includeStart,
        @JsonProperty("includeEnd") boolean includeEnd,
        @JsonProperty("prefixScan") boolean prefixScan
    ) {
        super(tableId, partId, schema, keyMapping, filter, selection);
        this.startKey = startKey;
        this.endKey = endKey;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
        this.prefixScan = prefixScan;
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        return part.getIteratorByRange(startKey, endKey, includeStart, includeEnd, prefixScan);
    }
}
