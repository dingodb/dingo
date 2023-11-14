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

package io.dingodb.exec.codec;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestAvroTupleCodec {
    public static @NonNull Stream<Arguments> getArguments() {
        return Stream.of(
            arguments(
                DingoTypeFactory.INSTANCE.tuple("INT", "STRING", "DOUBLE"),
                ImmutableList.of(
                    new Object[]{1, "Alice", 3.5},
                    new Object[]{2, "Betty", 3.6},
                    new Object[]{3, "Cindy", 3.7}
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("getArguments")
    public void testCodec(DingoType type, List<Object[]> tuples) throws IOException {
        AvroTupleCodec codec = new AvroTupleCodec(type);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        codec.encode(bos, tuples);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        List<Object[]> decodedTuples = codec.decode(bis);
        assertThat(decodedTuples).containsExactlyElementsOf(tuples);
    }
}
