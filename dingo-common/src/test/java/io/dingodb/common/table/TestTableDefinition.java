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

import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.Type;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTableDefinition {
    private static TableDefinition tableDefinition;

    @BeforeAll
    public static void setupAll() throws IOException {
        tableDefinition = TableDefinition.readJson(
            TestTableDefinition.class.getResourceAsStream("/table-test.json")
        );
    }

    @Test
    public void testGetAvroSchemaOfKey() {
        Schema schema = tableDefinition.getAvroSchemaOfKey();
        assertThat(schema.getFields().get(0).name()).isEqualTo("_0");
        Schema colSchema = schema.getFields().get(0).schema();
        assertThat(colSchema.getType()).isEqualTo(Schema.Type.INT);
    }

    @Test
    public void testGetAvroSchemaOfValue() {
        Schema schema = tableDefinition.getAvroSchemaOfValue();
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

    @Test
    public void testGetDingoSchema() {
        List<DingoSchema> schemaList = tableDefinition.getDingoSchema();
        assertThat(schemaList.get(0).getType()).isEqualTo(Type.INTEGER);
        assertThat(schemaList.get(0).getIndex()).isEqualTo(0);
        assertThat(schemaList.get(1).getType()).isEqualTo(Type.STRING);
        assertThat(schemaList.get(1).getIndex()).isEqualTo(1);
        assertThat(schemaList.get(2).getType()).isEqualTo(Type.DOUBLE);
        assertThat(schemaList.get(2).getIndex()).isEqualTo(2);
    }

    @Test
    public void testGetDingoSchemaOfKey() {
        List<DingoSchema> schemaList = tableDefinition.getDingoSchemaOfKey();
        assertThat(schemaList.get(0).getType()).isEqualTo(Type.INTEGER);
        assertThat(schemaList.get(0).getIndex()).isEqualTo(0);
    }

    @Test
    public void testGetDingoSchemaOfValue() {
        List<DingoSchema> schemaList = tableDefinition.getDingoSchemaOfValue();
        assertThat(schemaList.get(0).getType()).isEqualTo(Type.STRING);
        assertThat(schemaList.get(0).getIndex()).isEqualTo(0);
        assertThat(schemaList.get(1).getType()).isEqualTo(Type.DOUBLE);
        assertThat(schemaList.get(1).getIndex()).isEqualTo(1);
    }

    @Test
    public void testToJsonFromJson() throws IOException {
        String json = tableDefinition.toJson();
        System.out.println(json);
        TableDefinition definition = TableDefinition.fromJson(json);
        assertThat(definition).isEqualTo(tableDefinition);
    }

    @Test
    public void testWriteJsonReadJson() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        tableDefinition.writeJson(os);
        TableDefinition definition = TableDefinition.readJson(new ByteArrayInputStream(os.toByteArray()));
        assertThat(definition).isEqualTo(tableDefinition);
    }
}
