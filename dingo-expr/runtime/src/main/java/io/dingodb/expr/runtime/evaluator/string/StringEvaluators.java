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

package io.dingodb.expr.runtime.evaluator.string;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.StringEvaluator;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;

import javax.annotation.Nonnull;

@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = StringEvaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {Integer.class, Long.class}
)
final class StringEvaluators {
    private StringEvaluators() {
    }

    @Nonnull
    static String substring(@Nonnull String str, int begin, int end) {
        return str.substring(begin, end);
    }

    @Nonnull
    static String substring(@Nonnull String str, int begin) {
        return str.substring(begin);
    }

    static String maxstring(String str1, String str2) {
        if (str1 == null && str2 == null) {
            return str1;
        } else if (str1 == null) {
            return str2;
        } else if (str2 == null) {
            return str1;
        } else {
            return str1.compareTo(str2) >= 0 ? str1 : str2;
        }
    }

    static String minstring(String str1, String str2) {
        if (str1 == null && str2 == null) {
            return str1;
        } else if (str1 == null) {
            return str1;
        } else if (str2 == null) {
            return str2;
        } else {
            return str1.compareTo(str2) < 0 ? str1 : str2;
        }
    }
}
