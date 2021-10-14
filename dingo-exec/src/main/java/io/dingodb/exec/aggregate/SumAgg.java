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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.expr.runtime.TypeCodes;
import io.dingodb.expr.runtime.evaluator.arithmetic.AddEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import javax.annotation.Nonnull;

@JsonTypeName("sum")
public class SumAgg extends AbstractAgg {
    @JsonProperty("index")
    private final int index;

    @JsonCreator
    public SumAgg(@JsonProperty("index") int index) {
        this.index = index;
    }

    // TODO: choose evaluator in compiling time.
    private static Object doSum(Object v0, Object v1) {
        EvaluatorKey evaluatorKey = EvaluatorKey.of(TypeCodes.getTypeCode(v0), TypeCodes.getTypeCode(v1));
        try {
            Evaluator evaluator = AddEvaluatorFactory.INSTANCE.getEvaluator(evaluatorKey);
            return evaluator.eval(new Object[]{v0, v1});
        } catch (FailGetEvaluator e) {
            e.printStackTrace();
        }
        return v0;
    }

    @Override
    public Object init() {
        return 0L;
    }

    @Override
    public Object add(Object var, @Nonnull Object[] tuple) {
        return doSum(var, tuple[index]);
    }

    @Override
    public Object merge(Object var1, Object var2) {
        return doSum(var1, var2);
    }

    @Override
    public Object getValue(Object var) {
        return var;
    }
}
