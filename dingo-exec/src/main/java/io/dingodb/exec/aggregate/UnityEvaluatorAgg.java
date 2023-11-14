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

package io.dingodb.exec.aggregate;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.type.DingoType;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.BinaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class UnityEvaluatorAgg extends UnityAgg {
    @JsonProperty("type")
    protected final DingoType type;

    private BinaryOp op;

    protected UnityEvaluatorAgg(Integer index, @NonNull DingoType type) {
        super(index);
        this.type = type;
    }

    protected void setOp(@NonNull BinaryOp op) {
        this.op = op;
    }

    protected Object eval(Object v0, Object v1) {
        return op.eval(Exprs.val(v0), Exprs.val(v1), null, null);
    }

    @Override
    public Object first(Object @NonNull [] tuple) {
        return tuple[index];
    }

    @Override
    public Object add(@NonNull Object var, Object @NonNull [] tuple) {
        Object value = tuple[index];
        if (value != null) {
            return eval(var, value);
        }
        return var;
    }

    @Override
    public Object merge(@Nullable Object var1, @Nullable Object var2) {
        if (var1 != null) {
            if (var2 != null) {
                return eval(var1, var2);
            }
            return var1;
        }
        return var2;
    }

    @Override
    public Object getValue(Object var) {
        return var;
    }
}
