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

package io.dingodb.expr.runtime.evaluator.base;

import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class EvaluatorFactory implements Serializable {
    private static final long serialVersionUID = 5023860384673216809L;

    protected final Map<EvaluatorKey, Evaluator> evaluators;

    protected EvaluatorFactory() {
        evaluators = new HashMap<>();
    }

    /**
     * Get the Evaluator of a specified EvaluatorKey. A EvaluatorKey is a combination of the type code of parameters.
     * This method would try to return the universal evaluator if there is not a Evaluator for the specified
     * EvaluatorKey.
     *
     * @param key the EvaluatorKey
     * @return the Evaluator
     * @throws FailGetEvaluator if there is no appropriate Evaluator
     */
    public final @NonNull Evaluator getEvaluator(EvaluatorKey key) throws FailGetEvaluator {
        Evaluator evaluator = evaluators.get(key);
        if (evaluator != null) {
            return evaluator;
        }
        List<EvaluatorKey> keys = key.generalize();
        // The first one is copy of `key`
        for (int i = 1; i < keys.size(); ++i) {
            evaluator = evaluators.get(keys.get(i));
            if (evaluator != null) {
                return evaluator;
            }
        }
        throw new FailGetEvaluator(this, key.getParaTypeCodes());
    }
}
