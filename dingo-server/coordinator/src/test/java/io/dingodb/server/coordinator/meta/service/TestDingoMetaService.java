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

package io.dingodb.server.coordinator.meta.service;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.server.coordinator.TestBase;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoMetaService extends TestBase {

    private static TableDefinition definition;

    //@BeforeAll
    public static void beforeAll() throws Exception {
        TestBase.beforeAll();
        definition = new TableDefinition("TestTableAdaptor_testCreate");
        definition.setColumns(Arrays.asList(
            ColumnDefinition.builder().name("col1").type(SqlTypeName.INTEGER).build(),
            ColumnDefinition.builder().name("col2").type(SqlTypeName.VARCHAR).precision(32).build(),
            ColumnDefinition.builder().name("col3").type(SqlTypeName.INTEGER).build()
        ));
    }

    //@Test
    public void test00Create() {
        DingoMetaService.instance().createTable(definition.getName(), definition);
        assertThat(DingoMetaService.instance().getTableDefinition(definition.getName()))
            .isEqualTo(definition);
    }

    //@Test
    public void test01GetParts() {
        System.out.println(DingoMetaService.instance().getParts(definition.getName()));
        assertThat(DingoMetaService.instance().getParts(definition.getName()))
            .hasSizeGreaterThanOrEqualTo(1);
        assertThat(DingoMetaService.instance().getDistributes(definition.getName()))
            .hasSizeGreaterThanOrEqualTo(1);
    }

}
