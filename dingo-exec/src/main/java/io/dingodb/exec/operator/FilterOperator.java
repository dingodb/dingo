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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.expr.RtExprWithType;
import io.dingodb.exec.fin.Fin;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;

@JsonTypeName("filter")
@JsonPropertyOrder({"filter", "schema", "output"})
public final class FilterOperator extends SoleOutOperator {
    @JsonProperty("filter")
    private final RtExprWithType filter;
    @JsonProperty("schema")
    private final TupleSchema schema;

    @JsonCreator
    public FilterOperator(
        @JsonProperty("filter") RtExprWithType filter,
        @JsonProperty("schema") TupleSchema schema
    ) {
        super();
        this.filter = filter;
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        filter.compileIn(schema);
    }

    @Override
    public synchronized boolean push(int pin, Object[] tuple) {
        if (RtLogicalOp.test(filter.eval(new TupleEvalContext(tuple)))) {
            return output.push(tuple);
        }
        return true;
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        output.fin(fin);
    }
}
