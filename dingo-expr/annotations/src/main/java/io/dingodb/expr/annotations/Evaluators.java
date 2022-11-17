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

package io.dingodb.expr.annotations;

import io.dingodb.expr.core.evaluator.Evaluator;
import io.dingodb.expr.core.evaluator.EvaluatorFactory;
import io.dingodb.expr.core.evaluator.EvaluatorKey;
import io.dingodb.expr.core.evaluator.UniversalEvaluator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Evaluators {
    /**
     * Specify the EvaluatorKey class.
     *
     * @return the EvaluatorKey class
     */
    Class<?> evaluatorKey() default EvaluatorKey.class;

    /**
     * Specify the base class/interface of the generated Evaluators.
     *
     * @return the base class/interface
     */
    Class<?> evaluatorBase() default Evaluator.class;

    /**
     * Specify the base class of the generated EvaluatorFactory.
     *
     * @return the EvaluatorFactory base class
     */
    Class<?> evaluatorFactory() default EvaluatorFactory.class;

    /**
     * Specify the class of universal evaluator.
     *
     * @return the class of universal evaluator
     */
    Class<?> universalEvaluator() default UniversalEvaluator.class;

    /**
     * Specify the sequence of the type when inducing evaluators by type.
     *
     * @return an array of class
     */
    Class<?>[] induceSequence() default {};

    boolean checkRange() default false;
}
