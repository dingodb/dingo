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
import io.dingodb.expr.runtime.exception.ElementsNullNotAllowed;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

public final class RtSqlListConstructorOp extends RtOp {
    private static final long serialVersionUID = 8741536350543031890L;

    public RtSqlListConstructorOp(RtExpr[] paras) {
        super(paras);
    }

    @Override
    public @NonNull Object eval(EvalContext etx) throws FailGetEvaluator {
        int size = paras.length;
        List<Object> result = new ArrayList<>(size);
        for (RtExpr para : paras) {
            Object v = para.eval(etx);
            if (v == null) {
                throw new ElementsNullNotAllowed();
            }
            result.add(v);
        }
        return result;
    }

    @Override
    public int typeCode() {
        return TypeCode.LIST;
    }
}
