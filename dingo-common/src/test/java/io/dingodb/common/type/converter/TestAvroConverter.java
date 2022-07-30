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

package io.dingodb.common.type.converter;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAvroConverter {
    @Test
    public void testAvroSchema() {
        DingoType type = DingoTypeFactory.tuple("INT");
        Schema schema = type.toAvroSchema();
        assertThat(schema.getFields().get(0).name()).isEqualTo("_0");
        Schema colSchema = schema.getFields().get(0).schema();
        assertThat(colSchema.getType()).isEqualTo(Schema.Type.INT);
    }

    @Test
    public void testAvroSchema2() {
        DingoType type = DingoTypeFactory.tuple("STRING", "DOUBLE|NULL");
        Schema schema = type.toAvroSchema();
        assertThat(schema.getFields().get(0).name()).isEqualTo("_0");
        Schema colSchema = schema.getFields().get(0).schema();
        assertThat(colSchema.getType()).isEqualTo(Schema.Type.STRING);
        assertThat(schema.getFields().get(0).defaultVal()).isNull();
        assertThat(schema.getFields().get(1).name()).isEqualTo("_1");
        colSchema = schema.getFields().get(1).schema();
        assertThat(colSchema.getType()).isEqualTo(Schema.Type.UNION);
        assertThat(colSchema.getTypes())
            .map(Schema::getType)
            .containsExactly(Schema.Type.DOUBLE, Schema.Type.NULL);
        assertThat(schema.getFields().get(1).defaultVal()).isNull();
    }
}
