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

package io.dingodb.common.codec;

import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAvroCodec {
    private static AvroCodec codec;

    @BeforeAll
    public static void setupAll() throws IOException {
        TableDefinition tableDefinition = TableDefinition.readJson(
            TestAvroCodec.class.getResourceAsStream("/table-test.json")
        );
        List<Integer> index = new ArrayList<>();
        index.add(0);
        TupleMapping tupleMapping = TupleMapping.of(index);
        codec = new AvroCodec(tableDefinition.getDingoType().select(tupleMapping).toAvroSchema());
    }

    @Test
    public void testEncodeDecode() throws IOException {
        Object[] tuple = new Object[]{1, '~'};
        byte[] bytes = codec.encode(tuple);
        Object[] result = codec.decode(bytes);
        assertThat(result[0]).isEqualTo(1);
        assertThat(result[1]).isEqualTo(new Utf8("Alice"));
        assertThat(result[2]).isEqualTo(1.0);
    }
}
