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
import io.dingodb.common.table.ElementSchema;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class UnityEvaluatorAgg extends UnityAgg {
    @JsonProperty("type")
    protected final ElementSchema type;

    private Evaluator evaluator;

    protected UnityEvaluatorAgg(Integer index, @Nonnull ElementSchema type) {
        super(index);
        this.type = type;
    }

    protected void setEvaluator(@Nonnull EvaluatorFactory factory) {
        EvaluatorKey evaluatorKey = EvaluatorKey.of(type.getTypeCode(), type.getTypeCode());
        try {
            evaluator = factory.getEvaluator(evaluatorKey);
        } catch (FailGetEvaluator e) {
            throw new RuntimeException(e);
        }
    }

    protected Object eval(Object v0, Object v1) {
        try {
            return evaluator.eval(new Object[]{v0, v1});
        } catch (FailGetEvaluator e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object first(@Nonnull Object[] tuple) {
        return tuple[index];
    }

    @Override
    public Object add(@Nonnull Object var, @Nonnull Object[] tuple) {
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
