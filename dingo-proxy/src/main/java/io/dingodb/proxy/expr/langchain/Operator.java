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

package io.dingodb.proxy.expr.langchain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.sdk.service.entity.common.Schema;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.util.Iterator;
import java.util.List;

import static io.dingodb.expr.runtime.ExprConfig.SIMPLE;

@Data
@NoArgsConstructor
@FieldNameConstants
@JsonTypeName("operator")
public class Operator implements Expr {
    @JsonProperty("operator")
    private OpType operator;
    @JsonProperty("arguments")
    private List<Expr> arguments;

    @Override
    public io.dingodb.expr.runtime.expr.Expr toDingoExpr(
        List<String> attrNames, List<Schema> attrSchemas, List<Type> attrTypes
    ) {
        Iterator<Expr> iterator = arguments.iterator();
        switch (operator) {
            case NOT:
                checkArgs(iterator, operator);
                return Exprs.NOT.compile(iterator.next().toDingoExpr(attrNames, attrSchemas, attrTypes), SIMPLE);
            case AND: {
                io.dingodb.expr.runtime.expr.Expr[] operands = arguments.stream()
                    .map(arg -> arg.toDingoExpr(attrNames, attrSchemas, attrTypes))
                    .toArray(io.dingodb.expr.runtime.expr.Expr[]::new);
                return Exprs.AND_FUN.compile(operands, SIMPLE);
            }
            case OR: {
                io.dingodb.expr.runtime.expr.Expr[] operands = arguments.stream()
                    .map(arg -> arg.toDingoExpr(attrNames, attrSchemas, attrTypes))
                    .toArray(io.dingodb.expr.runtime.expr.Expr[]::new);
                return Exprs.OR_FUN.compile(operands, SIMPLE);
            }
            default:
                throw new IllegalStateException("Unexpected value: " + operator);
        }
    }

}
