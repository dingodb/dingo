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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.fun.DingoFunFactory;
import io.dingodb.exec.type.converter.ExprConverter;
import io.dingodb.expr.coding.CodingFlag;
import io.dingodb.expr.coding.ExprCoder;
import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.ExprCompileException;
import io.dingodb.expr.runtime.expr.Expr;
import lombok.Getter;

import java.io.ByteArrayOutputStream;

public class SqlExpr {
    private static final ExprParser EXPR_PARSER = new ExprParser(DingoFunFactory.getInstance());

    @JsonProperty("expr")
    @Getter
    private final String exprString;
    @JsonProperty("type")
    private final DingoType type;
    private transient SqlExprEvalContext etx = new SqlExprEvalContext();
    private transient Expr expr;

    @JsonCreator
    public SqlExpr(
        @JsonProperty("expr") String exprString,
        @JsonProperty("type") DingoType type
    ) {
        this.exprString = exprString;
        this.type = type;
        // TODO: Runtime env
//        this.etx = new SqlExprEvalContext();
    }

    private Expr getExpr() throws ExprParseException {
        return EXPR_PARSER.parse(exprString);
    }

    public byte[] getCoding(DingoType tupleType, DingoType parasType) {
        try {
            compileIn(tupleType, parasType);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if (ExprCoder.INSTANCE.visit(expr, os) == CodingFlag.OK) {
                return os.toByteArray();
            }
            return null;
        } catch (ExprCompileException e) {
            return null;
        }
    }

    public void compileIn(DingoType tupleType, DingoType parasType) {
        try {
            CompileContext context = new SqlExprCompileContext(tupleType, parasType);
            expr = ExprCompiler.ADVANCED.visit(getExpr(), context);
            etx = new SqlExprEvalContext();
        } catch (ExprParseException | ExprCompileException e) {
            throw new IllegalStateException(e);
        }
    }

    public void setParas(Object[] paras) {
        etx.setParas(paras);
    }

    public Object eval(Object[] tuple) {
        etx.setTuple(tuple);
        return type.convertFrom(expr.eval(etx, ExprConfig.ADVANCED), ExprConverter.INSTANCE);
    }

    public SqlExpr copy() {
        return new SqlExpr(exprString, type);
    }
}
