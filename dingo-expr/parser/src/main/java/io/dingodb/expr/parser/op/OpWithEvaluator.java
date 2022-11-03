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

import io.dingodb.expr.core.evaluator.Evaluator;
import io.dingodb.expr.core.evaluator.EvaluatorFactory;
import io.dingodb.expr.core.evaluator.EvaluatorKey;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtEvaluatorOp;
import io.dingodb.expr.runtime.op.RtOp;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

public class OpWithEvaluator extends Op {
    @Getter
    private final EvaluatorFactory factory;

    public OpWithEvaluator(OpType type, EvaluatorFactory factory) {
        super(type);
        this.factory = factory;
    }

    public OpWithEvaluator(String name, EvaluatorFactory factory) {
        super(name);
        this.factory = factory;
    }

    @Override
    protected @NonNull RtOp createRtOp(RtExpr[] rtExprArray) {
        int[] typeCodes = Arrays.stream(rtExprArray).mapToInt(RtExpr::typeCode).toArray();
        Evaluator evaluator = factory.getEvaluator(EvaluatorKey.of(typeCodes));
        return new RtEvaluatorOp(evaluator, rtExprArray);
    }
}
