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

package io.dingodb.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.table.TupleSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCsvUtils {
    private static TupleSchema schema;

    @BeforeAll
    public static void setupAll() throws IOException {
        TableDefinition tableDefinition = TableDefinition.readJson(
            TestCsvUtils.class.getResourceAsStream("/table-test.json")
        );
        schema = tableDefinition.getTupleSchema();
    }

    @Test
    public void testReadCsv() throws IOException {
        Iterator<Object[]> it = CsvUtils.readCsv(
            schema,
            getClass().getResourceAsStream("/table-test-data.csv")
        );
        int id = 1;
        while (it.hasNext()) {
            Object[] values = it.next();
            assertThat(values[0]).isInstanceOf(Integer.class);
            assertThat(values[0]).isEqualTo(id);
            ++id;
            assertThat(values[1]).isInstanceOf(String.class);
            assertThat(values[2]).isInstanceOf(Double.class);
        }
    }

    @Test
    public void testReadCsvWithSchema() throws IOException {
        Iterator<Object[]> it = CsvUtils.readCsvWithSchema(
            getClass().getResourceAsStream("/table-test-data-with-schema.csv")
        );
        int id = 1;
        while (it.hasNext()) {
            Object[] values = it.next();
            assertThat(values[0]).isInstanceOf(Integer.class);
            assertThat(values[0]).isEqualTo(id);
            ++id;
            assertThat(values[1]).isInstanceOf(String.class);
            assertThat(values[2]).isInstanceOf(Double.class);
        }
    }

    @Test
    public void testReadCsvLine() throws JsonProcessingException {
        List<Object[]> values = CsvUtils.readCsv(schema, "1, Alice, 1.0\n2, Betty, 2.0");
        Object[] tuple = values.get(0);
        assertThat(tuple[0]).isEqualTo(1);
        assertThat(tuple[1]).isEqualTo("Alice");
        assertThat(tuple[2]).isEqualTo(1.0);
        tuple = values.get(1);
        assertThat(tuple[0]).isEqualTo(2);
        assertThat(tuple[1]).isEqualTo("Betty");
        assertThat(tuple[2]).isEqualTo(2.0);
    }
}
