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
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.sql.RtSqlMapConstructorOp;

import javax.annotation.Nonnull;

public final class SqlMapConstructorOp extends Op {
    public static final String FUN_NAME = "MAP";

    private SqlMapConstructorOp() {
        super(FUN_NAME);
    }

    @Nonnull
    public static SqlMapConstructorOp fun() {
        return new SqlMapConstructorOp();
    }

    @Override
    protected boolean evalNull(@Nonnull RtExpr[] rtExprArray) {
        checkNoNulls(rtExprArray);
        return false;
    }

    @Nonnull
    @Override
    protected RtSqlMapConstructorOp createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator {
        return new RtSqlMapConstructorOp(rtExprArray);
    }
}
