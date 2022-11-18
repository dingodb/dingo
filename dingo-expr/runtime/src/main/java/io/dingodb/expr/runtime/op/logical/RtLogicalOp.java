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

package io.dingodb.expr.runtime.op.logical;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.core.evaluator.EvaluatorKey;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.evaluator.cast.BooleanCastEvaluatorsFactory;
import io.dingodb.expr.runtime.op.RtOp;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class RtLogicalOp extends RtOp {
    private static final long serialVersionUID = 5800304351907769891L;

    protected RtLogicalOp(RtExpr[] paras) {
        super(paras);
    }

    public static boolean test(@Nullable Object value) {
        if (value == null) {
            return false;
        }
        Object result = BooleanCastEvaluatorsFactory.INSTANCE
            .getEvaluator(EvaluatorKey.UNIVERSAL)
            .eval(new Object[]{value});
        assert result != null;
        return (boolean) result;
    }

    @Override
    public final int typeCode() {
        return TypeCode.BOOL;
    }
}
