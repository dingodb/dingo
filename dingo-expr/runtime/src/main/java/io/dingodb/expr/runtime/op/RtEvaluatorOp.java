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

package io.dingodb.expr.runtime.op;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import javax.annotation.Nonnull;

public class RtEvaluatorOp extends RtFun {
    private static final long serialVersionUID = -2145574267641248415L;
    @Nonnull
    private final Evaluator evaluator;

    /**
     * Create an RtEvaluatorOp. RtEvaluatorOp delegates the {@code eval} method to an Evaluator.
     *
     * @param evaluator the Evaluator
     * @param paras     the parameters of the op
     */
    public RtEvaluatorOp(@Nonnull Evaluator evaluator, @Nonnull RtExpr[] paras) {
        super(paras);
        this.evaluator = evaluator;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) throws FailGetEvaluator {
        return evaluator.eval(values);
    }

    @Override
    public final int typeCode() {
        return evaluator.typeCode();
    }
}
