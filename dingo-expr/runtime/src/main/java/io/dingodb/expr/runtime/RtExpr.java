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

package io.dingodb.expr.runtime;

import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import java.io.Serializable;
import javax.annotation.Nullable;

public interface RtExpr extends Serializable {
    /**
     * Evaluate the result of this RtExpr in a specified EvalContext.
     *
     * @param etx the specified EvalContext
     * @return the result
     * @throws FailGetEvaluator if there is no appropriate Evaluator
     */
    @Nullable
    Object eval(@Nullable EvalContext etx) throws FailGetEvaluator;

    /**
     * Get the type code of results. Must return they type code without call <code>eval</code> for compiling use.
     *
     * @return the type code
     */
    int typeCode();
}
