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
import io.dingodb.expr.runtime.op.logical.RtIsNotNull;

import javax.annotation.Nonnull;

public final class IsNotNullOp extends Op {
    private IsNotNullOp(OpType type) {
        super(type);
        name = "IS_NOT_NULL";
    }

    @Nonnull
    public static IsNotNullOp fun() {
        return new IsNotNullOp(OpType.FUN);
    }

    @Nonnull
    @Override
    protected RtIsNotNull createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator {
        return new RtIsNotNull(rtExprArray);
    }

    @Nonnull
    @Override
    protected RtExpr evalNullConst(@Nonnull RtExpr[] rtExprArray) throws DingoExprCompileException {
        RtExpr rtExpr = rtExprArray[0];
        try {
            if (rtExpr instanceof RtNull) {
                return RtConst.FALSE;
            }
            if (rtExpr instanceof RtConst) {
                Object v = rtExpr.eval(null);
                return v != null ? RtConst.TRUE : RtConst.FALSE;
            }
            return createRtOp(rtExprArray);
        } catch (FailGetEvaluator e) {
            throw new DingoExprCompileException(e);
        }
    }
}
