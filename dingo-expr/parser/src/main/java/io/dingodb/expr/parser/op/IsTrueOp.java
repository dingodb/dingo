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

import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import io.dingodb.expr.runtime.op.logical.RtIsTrue;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class IsTrueOp extends Op {
    public static final String FUN_NAME = "IS_TRUE";

    private IsTrueOp() {
        super(FUN_NAME);
    }

    public static @NonNull IsTrueOp fun() {
        return new IsTrueOp();
    }

    @Override
    protected @NonNull RtExpr evalNullConst(
        RtExpr @NonNull [] rtExprArray,
        @Nullable EvalEnv env
    ) {
        RtExpr rtExpr = rtExprArray[0];
        if (rtExpr instanceof RtConst || rtExpr instanceof RtNull) {
            Object v = rtExpr.eval(null);
            return RtLogicalOp.test(v) ? RtConst.TRUE : RtConst.FALSE;
        }
        return createRtOp(rtExprArray);
    }

    @Override
    protected @NonNull RtIsTrue createRtOp(RtExpr[] rtExprArray) {
        return new RtIsTrue(rtExprArray);
    }
}
