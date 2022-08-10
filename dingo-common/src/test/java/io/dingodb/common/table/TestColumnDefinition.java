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

package io.dingodb.common.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.expr.json.runtime.Parser;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestColumnDefinition {
    @Test
    public void testBuilder() {
        ColumnDefinition definition = ColumnDefinition.builder()
            .name("test")
            .type(SqlTypeName.INTEGER)
            .build();
        assertThat(definition)
            .hasFieldOrPropertyWithValue("name", "test")
            .hasFieldOrPropertyWithValue("type", SqlTypeName.INTEGER)
            .hasFieldOrPropertyWithValue("precision", RelDataType.PRECISION_NOT_SPECIFIED)
            .hasFieldOrPropertyWithValue("scale", RelDataType.SCALE_NOT_SPECIFIED)
            .hasFieldOrPropertyWithValue("notNull", false)
            .hasFieldOrPropertyWithValue("primary", false)
            .hasFieldOrPropertyWithValue("defaultValue", null);
    }

    @Nonnull
    public static Stream<Arguments> argumentsForTestSerDes() {
        return Stream.of(
            arguments(
                SqlTypeName.INTEGER,
                null,
                false,
                RelDataType.PRECISION_NOT_SPECIFIED,
                RelDataType.SCALE_NOT_SPECIFIED
            ),
            arguments(
                SqlTypeName.DECIMAL,
                null,
                true,
                7,
                3
            ),
            arguments(
                SqlTypeName.MULTISET,
                SqlTypeName.INTEGER,
                false,
                RelDataType.PRECISION_NOT_SPECIFIED,
                RelDataType.SCALE_NOT_SPECIFIED
            )
        );
    }

    @ParameterizedTest
    @MethodSource("argumentsForTestSerDes")
    public void testSerDes(
        SqlTypeName type,
        SqlTypeName elementType,
        boolean notNull,
        int precision,
        int scale
    ) throws JsonProcessingException {
        final Parser parser = Parser.JSON;
        ColumnDefinition definition = ColumnDefinition.builder()
            .name("test")
            .type(type)
            .elementType(elementType)
            .precision(precision)
            .scale(scale)
            .notNull(notNull)
            .build();
        String json = parser.stringify(definition);
        ColumnDefinition newDefinition = parser.parse(json, ColumnDefinition.class);
        assertThat(newDefinition).isEqualTo(definition);
    }
}
