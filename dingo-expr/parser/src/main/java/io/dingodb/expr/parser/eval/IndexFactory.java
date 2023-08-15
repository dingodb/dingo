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

package io.dingodb.expr.parser.eval;

import io.dingodb.expr.parser.exception.TemporarilyUnsupported;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.Evaluator;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class IndexFactory {
    private IndexFactory() {
    }

    public static @NonNull Eval of(Eval operand0, Eval operand1) {
        if (operand0 instanceof EvalStub) {
            EvalStub stub = (EvalStub) operand0;
            Object index = operand1.accept(Evaluator.of(null));
            return VarFactory.of(index, stub.getContext());
        }
        throw new TemporarilyUnsupported("INDEX");
    }
}
