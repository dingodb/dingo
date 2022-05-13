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
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestTypeCodesUniqueness {
    @Test
    public void testCodeUniqueness() {
        Class<?>[] classes = new Class[]{
            Boolean.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            BigDecimal.class,
            String.class,
            Object.class,
            Boolean[].class,
            Integer[].class,
            Long[].class,
            Float[].class,
            Double[].class,
            BigDecimal[].class,
            String[].class,
            Object[].class,
            List.class,
            Map.class,
            Date.class,
            Time.class,
            Timestamp.class,
        };
        Set<Integer> codeSet = new HashSet<>();
        for (Class<?> clazz : classes) {
            assertTrue(codeSet.add(TypeCodes.getTypeCode(clazz)));
        }
        assertTrue(codeSet.add(TypeCode.TUPLE));
        assertTrue(codeSet.add(TypeCode.DICT));
    }
}
