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

package io.dingodb.calcite.utils;

import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.ExprConverter;
import io.dingodb.exec.expr.SqlExprCompileContext;
import io.dingodb.exec.expr.SqlExprEvalContext;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.ExprCompileException;
import io.dingodb.expr.runtime.EvalEnv;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.TimeZone;

public final class CalcValueUtils {
    private CalcValueUtils() {
    }

    public static Object calcValue(
        RexNode rexNode,
        @NonNull DingoType targetType,
        Object[] tuple,
        DingoType tupleType,
        @Nullable EvalEnv env
    ) throws ExprCompileException {
        Expr expr = RexConverter.convert(rexNode);
        SqlExprEvalContext etx = new SqlExprEvalContext(env);
        etx.setTuple(tuple);
        return targetType.convertFrom(
            expr.compileIn(new SqlExprCompileContext(tupleType, null, env)).eval(etx),
            ExprConverter.INSTANCE
        );
    }

    public static Object @NonNull [] calcValues(
        @NonNull List<RexNode> rexNodeList,
        DingoType targetType,
        Object[] tuple,
        DingoType tupleType,
        @Nullable EvalEnv env
    ) throws ExprCompileException {
        int size = rexNodeList.size();
        Object[] result = new Object[size];
        for (int i = 0; i < size; ++i) {
            result[i] = calcValue(rexNodeList.get(i), targetType.getChild(i), tuple, tupleType, env);
        }
        return result;
    }

    public static @NonNull EvalEnv getEnv(@NonNull RelOptRuleCall call) {
        return EvalEnv.builder()
            .timeZone(call.getPlanner().getContext().unwrap(TimeZone.class))
            .build();
    }
}
