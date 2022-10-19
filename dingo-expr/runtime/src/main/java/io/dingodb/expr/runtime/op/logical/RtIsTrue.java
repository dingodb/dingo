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
import org.checkerframework.checker.nullness.qual.NonNull;

public class RtIsTrue extends RtLogicalOp {
    private static final long serialVersionUID = -3041596851911629158L;

    public RtIsTrue(RtExpr[] paras) {
        super(paras);
    }

    @Override
    public @NonNull Object eval(EvalContext etx) throws FailGetEvaluator {
        Object v = paras[0].eval(etx);
        return RtLogicalOp.test(v);
    }
}
