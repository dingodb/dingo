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

package io.dingodb.exec.fun;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.TertiaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;

public class IfFun extends TertiaryOp {
    @Serial
    private static final long serialVersionUID = -5133323746662125787L;

    public static final IfFun INSTANCE = new IfFun();

    public static final String NAME = "IF";

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2) {
        return OpKeys.BOOL_STRING_STRING.keyOf(type0, type1, type2);
    }

    @Override
    public Object evalValue(@NonNull Object value0, @NonNull Object value1, @NonNull Object value2, ExprConfig config) {
        if (value0 instanceof Boolean) {
            boolean v0 = (boolean) value0;
            if (v0) {
                return value1;
            } else {
                return value2;
            }
        } else {
            return value1;
        }
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
