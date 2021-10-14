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

package io.dingodb.expr.runtime.evaluator.index;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.evaluator.base.BooleanEvaluator;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.IntegerEvaluator;
import io.dingodb.expr.runtime.evaluator.base.LongEvaluator;
import io.dingodb.expr.runtime.evaluator.base.ObjectEvaluator;
import io.dingodb.expr.runtime.evaluator.base.StringEvaluator;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = Evaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {Integer.class, Long.class}
)
final class IndexEvaluators {
    private IndexEvaluators() {
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static Integer index(@Nonnull Integer[] array, int index) {
        return array[index];
    }

    @Evaluators.Base(LongEvaluator.class)
    static Long index(@Nonnull Long[] array, int index) {
        return array[index];
    }

    @Evaluators.Base(BooleanEvaluator.class)
    static Boolean index(@Nonnull Boolean[] array, int index) {
        return array[index];
    }

    @Evaluators.Base(StringEvaluator.class)
    static String index(@Nonnull String[] array, int index) {
        return array[index];
    }

    @Evaluators.Base(ObjectEvaluator.class)
    static Object index(@Nonnull Object[] array, int index) {
        return array[index];
    }

    @Evaluators.Base(ObjectEvaluator.class)
    static Object index(@Nonnull List<?> array, int index) {
        return array.get(index);
    }

    @Evaluators.Base(ObjectEvaluator.class)
    static Object index(@Nonnull Map<String, ?> map, String index) {
        return map.get(index);
    }
}
