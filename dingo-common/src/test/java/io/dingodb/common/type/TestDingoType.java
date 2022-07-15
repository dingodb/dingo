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

package io.dingodb.common.type;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.ObjectType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.json.runtime.Parser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestDingoType {
    private static final Parser PARSER = Parser.JSON;

    @Nonnull
    public static Stream<Arguments> getParametersForScalar() {
        return Stream.of(
            arguments(new BooleanType(false), "\"BOOL\""),
            arguments(new BooleanType(true), "\"BOOL|NULL\""),
            arguments(new DecimalType(false), "\"DECIMAL\""),
            arguments(new DecimalType(true), "\"DECIMAL|NULL\""),
            arguments(new DoubleType(false), "\"DOUBLE\""),
            arguments(new DoubleType(true), "\"DOUBLE|NULL\""),
            arguments(new IntegerType(false), "\"INT\""),
            arguments(new IntegerType(true), "\"INT|NULL\""),
            arguments(new LongType(false), "\"LONG\""),
            arguments(new LongType(true), "\"LONG|NULL\""),
            arguments(new ObjectType(false), "\"OBJECT\""),
            arguments(new ObjectType(true), "\"OBJECT|NULL\""),
            arguments(new StringType(false), "\"STRING\""),
            arguments(new StringType(true), "\"STRING|NULL\""),
            arguments(new DateType(false), "\"DATE\""),
            arguments(new DateType(true), "\"DATE|NULL\""),
            arguments(new TimeType(false), "\"TIME\""),
            arguments(new TimeType(true), "\"TIME|NULL\""),
            arguments(new TimestampType(false), "\"TIMESTAMP\""),
            arguments(new TimestampType(true), "\"TIMESTAMP|NULL\"")
        );
    }

    @Nonnull
    public static Stream<Arguments> getParametersForTuple() {
        return Stream.of(
            arguments(
                DingoTypeFactory.tuple("INT", "DOUBLE"),
                "[ \"INT\", \"DOUBLE\" ]"
            ),
            arguments(
                DingoTypeFactory.tuple("INT|NULL", "TIME"),
                "[ \"INT|NULL\", \"TIME\" ]"
            )
        );
    }

    @ParameterizedTest
    @MethodSource({"getParametersForScalar", "getParametersForTuple"})
    public void testSerialize(DingoType type, String typeString) throws IOException {
        assertThat(PARSER.stringify(type)).isEqualTo(typeString);
    }

    @ParameterizedTest
    @MethodSource({"getParametersForScalar", "getParametersForTuple"})
    public void testDeserialize(DingoType type, String typeString) throws JsonProcessingException {
        assertThat(PARSER.parse(typeString, DingoType.class)).isEqualTo(type);
    }
}
