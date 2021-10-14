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

package io.dingodb.expr.runtime.base;

import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluatorKey {
    @Test
    public void testGeneralize() {
        EvaluatorKey key = EvaluatorKey.of(TypeCode.INTEGER);
        List<EvaluatorKey> keys = key.generalize();
        assertThat(keys.size()).isEqualTo(3);
        assertThat(keys)
            .contains(EvaluatorKey.of(TypeCode.INTEGER))
            .contains(EvaluatorKey.of(TypeCode.OBJECT))
            .contains(EvaluatorKey.UNIVERSAL);
    }

    @Test
    public void testGeneralize2() {
        EvaluatorKey key = EvaluatorKey.of(TypeCode.INTEGER, TypeCode.LONG);
        List<EvaluatorKey> keys = key.generalize();
        assertThat(keys.size()).isEqualTo(5);
        assertThat(keys)
            .contains(EvaluatorKey.of(TypeCode.INTEGER, TypeCode.LONG))
            .contains(EvaluatorKey.of(TypeCode.INTEGER, TypeCode.OBJECT))
            .contains(EvaluatorKey.of(TypeCode.OBJECT, TypeCode.LONG))
            .contains(EvaluatorKey.of(TypeCode.OBJECT, TypeCode.OBJECT))
            .contains(EvaluatorKey.UNIVERSAL);
    }

    @Test
    public void testGeneralize3() {
        EvaluatorKey key = EvaluatorKey.of(TypeCode.INTEGER, TypeCode.LONG, TypeCode.DECIMAL);
        List<EvaluatorKey> keys = key.generalize();
        assertThat(keys.size()).isEqualTo(9);
        assertThat(keys)
            .contains(EvaluatorKey.of(TypeCode.INTEGER, TypeCode.LONG, TypeCode.DECIMAL))
            .contains(EvaluatorKey.of(TypeCode.INTEGER, TypeCode.LONG, TypeCode.OBJECT))
            .contains(EvaluatorKey.of(TypeCode.INTEGER, TypeCode.OBJECT, TypeCode.DECIMAL))
            .contains(EvaluatorKey.of(TypeCode.INTEGER, TypeCode.OBJECT, TypeCode.OBJECT))
            .contains(EvaluatorKey.of(TypeCode.OBJECT, TypeCode.LONG, TypeCode.DECIMAL))
            .contains(EvaluatorKey.of(TypeCode.OBJECT, TypeCode.LONG, TypeCode.OBJECT))
            .contains(EvaluatorKey.of(TypeCode.OBJECT, TypeCode.OBJECT, TypeCode.DECIMAL))
            .contains(EvaluatorKey.of(TypeCode.OBJECT, TypeCode.OBJECT, TypeCode.OBJECT))
            .contains(EvaluatorKey.UNIVERSAL);
    }
}
