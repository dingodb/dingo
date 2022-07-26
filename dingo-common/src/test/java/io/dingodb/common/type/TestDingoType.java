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

import io.dingodb.common.type.scalar.BinaryType;
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
            arguments(new BinaryType(false)),
            arguments(new BinaryType(true)),
            arguments(new BooleanType(false)),
            arguments(new BooleanType(true)),
            arguments(new DecimalType(false)),
            arguments(new DecimalType(true)),
            arguments(new DoubleType(false)),
            arguments(new DoubleType(true)),
            arguments(new IntegerType(false)),
            arguments(new IntegerType(true)),
            arguments(new LongType(false)),
            arguments(new LongType(true)),
            arguments(new ObjectType(false)),
            arguments(new ObjectType(true)),
            arguments(new StringType(false)),
            arguments(new StringType(true)),
            arguments(new DateType(false)),
            arguments(new DateType(true)),
            arguments(new TimeType(false)),
            arguments(new TimeType(true)),
            arguments(new TimestampType(false)),
            arguments(new TimestampType(true))
        );
    }

    @Nonnull
    public static Stream<Arguments> getParametersForComposite() {
        return Stream.of(
            arguments(DingoTypeFactory.tuple("INT", "DOUBLE")),
            arguments(DingoTypeFactory.tuple("INT|NULL", "TIME")),
            arguments(DingoTypeFactory.array("INT", false)),
            arguments(DingoTypeFactory.array("INT", true))
        );
    }

    @ParameterizedTest
    @MethodSource({"getParametersForScalar", "getParametersForComposite"})
    public void testSerDes(DingoType type) throws IOException {
        String json = PARSER.stringify(type);
        DingoType newType = PARSER.parse(json, AbstractDingoType.class);
        assertThat(newType).isEqualTo(type);
    }
}
