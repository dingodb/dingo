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

package io.dingodb.test.agg;

import io.dingodb.common.table.TupleSchema;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.test.SqlHelper;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.calcite.rel.core.Aggregate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryAgg2Test {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile(QueryAgg2Test.class.getResourceAsStream("table-people-create.sql"));
        sqlHelper.execFile(QueryAgg2Test.class.getResourceAsStream("table-people-data.sql"));
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testTypeMismatch() {
        // Disable assert in `Aggregate` to allow our own check in `DingoAggregate`.
        // but `gradle test` seems not respect to this and throws AssertionError occasionally.
        Aggregate.class.getClassLoader().clearAssertionStatus();
        Aggregate.class.getClassLoader().setClassAssertionStatus(Aggregate.class.getName(), false);
        assertFalse(Aggregate.class.desiredAssertionStatus());
        assertThrows(AssertionError.class, () -> {
            assertThrows(AvaticaSqlException.class, () -> {
                sqlHelper.queryTest(
                    "select avg(name) from people",
                    new String[]{"expr$0"},
                    TupleSchema.ofTypes(TypeCode.DECIMAL),
                    "107803.547\n"
                );
            });
        });
    }
}
