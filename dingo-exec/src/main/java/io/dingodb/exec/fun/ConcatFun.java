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
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.TertiaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;

public class ConcatFun extends TertiaryOp {

    public static final ConcatFun INSTANCE = new ConcatFun();

    public static final String NAME = "CONCAT";

    @Serial
    private static final long serialVersionUID = -6456730710140240892L;

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2) {
        return OpKeys.STRING_STRING_STRING.keyOf(type0, type1, type2);
    }

    @Override
    public Object evalValue(@NonNull Object value0, @NonNull Object value1, @NonNull Object value2, ExprConfig config) {
        if (value0 == null || value1 == null || value2 == null) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(value0).append(value1).append(value2);
        return stringBuilder.toString();
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
