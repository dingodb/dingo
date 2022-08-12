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

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtFun;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public final class RtSqlCastListItemsOp extends RtFun {
    private static final long serialVersionUID = -7968394097724524958L;

    @Nonnull
    private final Evaluator evaluator;

    public RtSqlCastListItemsOp(@Nonnull Evaluator evaluator, @Nonnull RtExpr[] paras) {
        super(paras);
        this.evaluator = evaluator;
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) throws FailGetEvaluator {
        List<?> list = (List<?>) values[0];
        List<Object> result = new ArrayList<>(list.size());
        for (Object i : list) {
            result.add(evaluator.eval(new Object[]{i}));
        }
        return result;
    }

    @Override
    public int typeCode() {
        return TypeCode.LIST;
    }
}
