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

package io.dingodb.expr.runtime.eval;

import io.dingodb.expr.core.TypeCode;

public abstract class FloatUnaryEval extends UnaryEval {
    private static final long serialVersionUID = -63126595303284005L;

    protected FloatUnaryEval(Eval operand) {
        super(operand);
    }

    @Override
    public int getType() {
        return TypeCode.FLOAT;
    }
}
