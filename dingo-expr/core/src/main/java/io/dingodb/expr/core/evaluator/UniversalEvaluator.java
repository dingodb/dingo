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

package io.dingodb.expr.core.evaluator;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.core.exception.FailGetEvaluator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UniversalEvaluator implements Evaluator {
    private static final long serialVersionUID = 8115905605402311713L;
    private final EvaluatorFactory factory;
    private final int typeCode;

    @Override
    public Object eval(Object[] paras) {
        int[] typeCodes = TypeCode.getTypeCodes(paras);
        Evaluator evaluator = factory.getEvaluator(EvaluatorKey.of(typeCodes));
        if (evaluator != this) {
            return evaluator.eval(paras);
        }
        throw new FailGetEvaluator(factory, typeCodes);
    }

    @Override
    public int typeCode() {
        return typeCode;
    }
}
