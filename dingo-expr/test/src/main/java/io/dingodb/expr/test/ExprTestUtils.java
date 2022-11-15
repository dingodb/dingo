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

package io.dingodb.expr.test;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.ExprCompileException;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import org.checkerframework.checker.nullness.qual.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public final class ExprTestUtils {
    private ExprTestUtils() {
    }

    public static void testEval(
        @NonNull DingoExprCompiler compiler,
        String exprString,
        Object value
    ) throws ExprParseException, ExprCompileException {
        Expr expr = compiler.parse(exprString);
        RtExpr rtExpr = expr.compileIn(null);
        assertThat(rtExpr).isInstanceOf(value != null ? RtConst.class : RtNull.class);
        Object result = rtExpr.eval(null);
        assertEquals(result, value);
    }

    public static void testEvalWithVar(
        @NonNull DingoExprCompiler compiler,
        @NonNull ExprContext context,
        int etxIndex,
        String exprString,
        Object value
    ) throws ExprParseException, ExprCompileException {
        Expr expr = compiler.parse(exprString);
        RtExpr rtExpr = expr.compileIn(context.getCtx());
        Object result = rtExpr.eval(context.getEtx(etxIndex));
        assertEquals(result, value);
    }

    public static void assertEquals(Object result, Object value) {
        if (result instanceof Double) {
            assertThat((Double) result).isCloseTo((Double) value, offset(1e-6));
        } else {
            assertThat(result).isEqualTo(value);
        }
    }
}
