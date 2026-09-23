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

package io.dingodb.exec.fun.mysql;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

/** MySQL LEAST for two numeric operands; SQL NULL propagates through BinaryOp. */
public class LeastFun extends BinaryOp {
    public static final String NAME = "least";
    private static final long serialVersionUID = -8602219486245764404L;
    public static final LeastFun INSTANCE = new LeastFun();

    @Override
    protected Object evalNonNullValue(@NonNull Object left, @NonNull Object right, ExprConfig config) {
        Number a = (Number) left;
        Number b = (Number) right;
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            BigDecimal x = new BigDecimal(a.toString());
            BigDecimal y = new BigDecimal(b.toString());
            return x.min(y);
        }
        if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
            return Math.min(a.doubleValue(), b.doubleValue());
        }
        return Math.min(a.longValue(), b.longValue());
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.ANY;
    }
}
