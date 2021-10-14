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

import java.io.Serializable;

public interface Evaluator extends Serializable {
    /**
     * Calculate the result of given parameter values.
     *
     * @param paras the given parameter values
     * @return the result
     * @throws FailGetEvaluator if there is no appropriate Evaluator for the given parameter types
     */
    Object eval(Object[] paras) throws FailGetEvaluator;

    /**
     * Get the type code of results. Must return they type code without call <code>eval</code> for compiling use.
     *
     * @return the type code
     */
    int typeCode();
}
