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

package io.dingodb.expr.runtime.op.sql;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtOp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class RtSqlCaseOp extends RtOp {
    private static final long serialVersionUID = 262253285071682317L;

    public RtSqlCaseOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.OBJECT;
    }

    @Nullable
    @Override
    public Object eval(@Nullable EvalContext etx) throws FailGetEvaluator {
        int size = paras.length;
        for (int i = 0; i < size - 1; i += 2) {
            RtExpr para = paras[i];
            Object v = para.eval(etx);
            if ((boolean) v) {
                return paras[i + 1].eval(etx);
            }
        }
        // There will be a `null` if you missed `ELSE` in SQL.
        return paras[size - 1].eval(etx);
    }
}
