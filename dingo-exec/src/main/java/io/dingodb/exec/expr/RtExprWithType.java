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

package io.dingodb.exec.expr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.table.ElementSchema;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.util.ExprUtil;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import lombok.Getter;

public class RtExprWithType {
    @JsonProperty("expr")
    @Getter
    private final String exprString;
    @JsonProperty("type")
    private final ElementSchema type;

    private RtExpr expr;

    @JsonCreator
    public RtExprWithType(
        @JsonProperty("expr") String exprString,
        @JsonProperty("type") ElementSchema type
    ) {
        this.exprString = exprString;
        this.type = type;
    }

    public void compileIn(TupleSchema schema) {
        expr = ExprUtil.compileExpr(exprString, schema);
    }

    public Object eval(EvalContext etx) {
        try {
            return type.convert(expr.eval(etx));
        } catch (FailGetEvaluator e) {
            throw new RuntimeException("Error occurred in evaluating expression \"" + exprString + "\".", e);
        }
    }
}
