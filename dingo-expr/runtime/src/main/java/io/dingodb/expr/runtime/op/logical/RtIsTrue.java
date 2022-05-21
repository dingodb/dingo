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

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RtIsTrue extends RtLogicalOp {
    private static final long serialVersionUID = -3041596851911629158L;

    public RtIsTrue(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nullable
    @Override
    public Object eval(@Nullable EvalContext etx) throws FailGetEvaluator {
        Object v = paras[0].eval(etx);
        return v != null && test(v);
    }
}
