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

package io.dingodb.exec.fun.special;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public final class CaseOp extends Op {
    public static final String NAME = "CASE";

    private CaseOp() {
        super(NAME);
    }

    public static @NonNull CaseOp fun() {
        return new CaseOp();
    }

    @Override
    protected @NonNull RtExpr evalNullConst(
        RtExpr @NonNull [] rtExprArray,
        @Nullable EvalEnv env
    ) {
        int size = rtExprArray.length;
        List<RtExpr> nonConstExprs = new ArrayList<>(size);
        for (int i = 0; i < size - 1; i += 2) {
            RtExpr rtExpr = rtExprArray[i];
            if (rtExpr instanceof RtNull) {
                continue;
            }
            if (rtExpr instanceof RtConst) {
                Object v = rtExpr.eval(null);
                if (RtLogicalOp.test(v)) {
                    return rtExprArray[i + 1];
                }
            } else {
                nonConstExprs.add(rtExpr);
                nonConstExprs.add(rtExprArray[i + 1]);
            }
        }
        if (nonConstExprs.size() == 0) {
            return rtExprArray[size - 1];
        }
        nonConstExprs.add(rtExprArray[size - 1]);
        return createRtOp(nonConstExprs.toArray(new RtExpr[0]));
    }

    @Override
    protected @NonNull Runtime createRtOp(RtExpr[] rtExprArray) {
        return new Runtime(rtExprArray);
    }

    public static final class Runtime extends RtOp {
        private static final long serialVersionUID = 262253285071682317L;

        public Runtime(RtExpr[] paras) {
            super(paras);
        }

        @Override
        public Object eval(EvalContext etx) {
            int size = paras.length;
            for (int i = 0; i < size - 1; i += 2) {
                RtExpr para = paras[i];
                Object v = para.eval(etx);
                if (RtLogicalOp.test(v)) {
                    return paras[i + 1].eval(etx);
                }
            }
            // There will be a `null` if you missed `ELSE` in SQL.
            return paras[size - 1].eval(etx);
        }

        @Override
        public int typeCode() {
            return TypeCode.OBJECT;
        }
    }
}
