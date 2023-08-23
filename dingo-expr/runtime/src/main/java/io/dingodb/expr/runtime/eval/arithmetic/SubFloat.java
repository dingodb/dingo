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

package io.dingodb.expr.runtime.eval.arithmetic;

import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.EvalVisitor;
import io.dingodb.expr.runtime.eval.FloatBinaryEval;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubFloat extends FloatBinaryEval {
    private static final long serialVersionUID = 4140501428391313567L;

    public SubFloat(Eval operand0, Eval operand1) {
        super(operand0, operand1);
    }

    @Override
    public <T> T accept(@NonNull EvalVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
