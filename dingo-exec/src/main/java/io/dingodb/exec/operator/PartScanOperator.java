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
import io.dingodb.common.table.TableId;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.util.ExprUtil;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import java.util.Iterator;

@JsonTypeName("scan")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "filter", "selection"})
public final class PartScanOperator extends PartIteratorSourceOperator {
    @JsonProperty("filters")
    private final String filter;
    @JsonProperty("selection")
    private final TupleMapping selection;

    private RtExpr filterExpr;

    @JsonCreator
    public PartScanOperator(
        @JsonProperty("table") TableId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") TupleSchema schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") String filter,
        @JsonProperty("selection") TupleMapping selection
    ) {
        super(tableId, partId, schema, keyMapping);
        this.filter = filter;
        this.selection = selection;
    }

    @Override
    public void init() {
        super.init();
        Iterator<Object[]> iterator = part.getIterator();
        if (filter != null) {
            filterExpr = ExprUtil.compileExpr(filter, schema);
            iterator = Iterators.filter(iterator, tuple -> {
                try {
                    return (boolean) filterExpr.eval(new TupleEvalContext(tuple));
                } catch (FailGetEvaluator e) {
                    e.printStackTrace();
                    return false;
                }
            });
        }
        if (selection != null) {
            iterator = Iterators.transform(iterator, selection::revMap);
        }
        this.iterator = iterator;
    }
}
