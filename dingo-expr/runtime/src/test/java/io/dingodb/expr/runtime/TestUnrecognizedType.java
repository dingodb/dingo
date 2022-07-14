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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestUnrecognizedType {
    @Test
    public void testUnrecognizedCode() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> TypeCode.nameOf(1));
        assertThat(exception.getMessage()).contains("1");
    }

    @Test
    public void testUnrecognizedName() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> TypeCode.codeOf("AAA"));
        assertThat(exception.getMessage()).contains("AAA");
    }
}
