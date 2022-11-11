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

package io.dingodb.expr.runtime.evaluator.mathematical;

import io.dingodb.expr.annotations.Evaluators;

import java.math.BigDecimal;

@Evaluators(
    induceSequence = {Double.class, BigDecimal.class, Long.class, Integer.class}
)
final class MathematicalEvaluators {
    private MathematicalEvaluators() {
    }

    static int abs(int num) {
        return Math.abs(num);
    }

    static long abs(long num) {
        return Math.abs(num);
    }

    static double abs(double num) {
        return Math.abs(num);
    }

    static double sin(double num) {
        return Math.sin(num);
    }

    static double cos(double num) {
        return Math.cos(num);
    }

    static double tan(double num) {
        return Math.tan(num);
    }

    static double asin(double num) {
        return Math.asin(num);
    }

    static double acos(double num) {
        return Math.acos(num);
    }

    static double atan(double num) {
        return Math.atan(num);
    }

    static double cosh(double num) {
        return Math.cosh(num);
    }

    static double sinh(double num) {
        return Math.sinh(num);
    }

    static double tanh(double num) {
        return Math.tanh(num);
    }

    static double log(double num) {
        return Math.log(num);
    }

    static double exp(double num) {
        return Math.exp(num);
    }
}
