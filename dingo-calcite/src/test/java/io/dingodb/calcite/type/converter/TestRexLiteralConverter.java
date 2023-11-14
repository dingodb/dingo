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

package io.dingodb.calcite.type.converter;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Time;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestRexLiteralConverter {
    public static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("00:00:00", new Time(0)),
            arguments("01:00:00", new Time(60 * 60 * 1000)),
            arguments("23:59:59.999", new Time(24 * 60 * 60 * 1000 - 1))
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String jdbcString, Object value) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelDataType relDataType = typeFactory.createSqlType(SqlTypeName.TIME);
        RexLiteral literal = RexLiteral.fromJdbcString(relDataType, SqlTypeName.TIME, jdbcString);
        DingoType type = DingoTypeFactory.INSTANCE.scalar("TIME", false);
        Time result = (Time) type.convertFrom(literal.getValue(), RexLiteralConverter.INSTANCE);
        assertThat(result).isEqualTo(value);
    }
}
