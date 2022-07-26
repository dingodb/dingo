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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestTypeCodesEquality {
    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            // scalar types
            arguments(Boolean.class, TypeCode.BOOL),
            arguments(Integer.class, TypeCode.INT),
            arguments(Long.class, TypeCode.LONG),
            arguments(Double.class, TypeCode.DOUBLE),
            arguments(BigDecimal.class, TypeCode.DECIMAL),
            arguments(String.class, TypeCode.STRING),
            arguments(Object.class, TypeCode.OBJECT),
            arguments(byte[].class, TypeCode.BINARY),
            // collection types
            arguments(Object[].class, TypeCode.ARRAY),
            arguments(List.class, TypeCode.LIST),
            arguments(LinkedList.class, TypeCode.LIST),
            arguments(ArrayList.class, TypeCode.LIST),
            arguments(Map.class, TypeCode.MAP),
            arguments(HashMap.class, TypeCode.MAP),
            arguments(TreeMap.class, TypeCode.MAP),
            arguments(LinkedHashMap.class, TypeCode.MAP),
            // date&time types
            arguments(Date.class, TypeCode.DATE),
            arguments(Time.class, TypeCode.TIME),
            arguments(Timestamp.class, TypeCode.TIMESTAMP),
            // null
            arguments(Void.class, TypeCode.NULL)
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testCodeEquality(Class<?> clazz, int typeCode) {
        assertEquals(TypeCodes.getTypeCode(clazz), typeCode);
    }

    @Test
    public void testTime() {
        assertEquals(TypeCodes.getTypeCode(Time.class), TypeCode.TIME);
    }
}
