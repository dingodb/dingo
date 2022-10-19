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

package io.dingodb.expr.parser.op;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestFunFactory {
    @Test
    @Order(0)
    public void testCallUnregistered() {
        assertThrows(DingoExprParseException.class,
            () -> DingoExprCompiler.parse("hello('world')"));
    }

    @Test
    @Order(1)
    public void testRegisterUdf() throws Exception {
        FunFactory.INS.registerUdf("hello", HelloOp::new);
        Expr expr = DingoExprCompiler.parse("hello('world')");
        RtExpr rtExpr = expr.compileIn(null);
        assertThat(rtExpr.eval(null)).isEqualTo("Hello world");
    }

    static class HelloOp extends RtFun {
        private static final long serialVersionUID = -8060697833705004059L;

        protected HelloOp(RtExpr @NonNull [] paras) {
            super(paras);
        }

        @Override
        protected Object fun(Object @NonNull [] values) {
            return "Hello " + values[0];
        }

        @Override
        public int typeCode() {
            return TypeCode.STRING;
        }
    }
}
