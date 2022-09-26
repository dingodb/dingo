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

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.sql.RtSqlCastListItemsOp;

import java.util.Objects;
import javax.annotation.Nonnull;

public final class SqlCastListItemsOp extends Op {
    public static final String FUN_NAME = "CAST_LIST_ITEMS";

    private SqlCastListItemsOp(OpType type) {
        super(type);
        name = FUN_NAME;
    }

    @Nonnull
    public static SqlCastListItemsOp fun() {
        return new SqlCastListItemsOp(OpType.FUN);
    }

    @Nonnull
    @Override
    protected RtSqlCastListItemsOp createRtOp(@Nonnull RtExpr[] rtExprArray) throws FailGetEvaluator {
        int toTypeCode = TypeCode.codeOf((String) Objects.requireNonNull(rtExprArray[0].eval(null)));
        // The types of array elements cannot be achieved, even not the same, so cast with universal evaluators.
        return new RtSqlCastListItemsOp(
            FunFactory.getCastEvaluatorFactory(toTypeCode).getEvaluator(EvaluatorKey.UNIVERSAL),
            new RtExpr[]{rtExprArray[1]}
        );
    }
}
