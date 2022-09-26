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

import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.sql.RtSqlArrayConstructorOp;

import java.util.Arrays;
import javax.annotation.Nonnull;

public final class SqlArrayConstructorOp extends Op {
    public static final String FUN_NAME = "ARRAY";

    private SqlArrayConstructorOp(OpType type) {
        super(type);
        name = FUN_NAME;
    }

    @Nonnull
    public static SqlArrayConstructorOp fun() {
        return new SqlArrayConstructorOp(OpType.FUN);
    }

    @Nonnull
    @Override
    protected RtExpr evalNullConst(@Nonnull RtExpr[] rtExprArray) throws DingoExprCompileException {
        try {
            // Check here so null values are caught even there are some non-const operands.
            if (Arrays.stream(rtExprArray).anyMatch(e -> e instanceof RtNull)) {
                throw new IllegalArgumentException("Null values are not allowed in arrays.");
            }
            RtOp rtOp = createRtOp(rtExprArray);
            if (Arrays.stream(rtExprArray).allMatch(e -> e instanceof RtConst)) {
                return new RtConst(rtOp.eval(null));
            }
            return rtOp;
        } catch (FailGetEvaluator e) {
            throw new DingoExprCompileException(e);
        }
    }

    @Nonnull
    @Override
    protected RtSqlArrayConstructorOp createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator {
        return new RtSqlArrayConstructorOp(rtExprArray);
    }
}
