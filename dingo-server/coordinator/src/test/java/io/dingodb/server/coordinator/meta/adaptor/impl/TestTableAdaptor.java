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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import io.dingodb.common.CommonId;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.server.coordinator.TestBase;
import io.dingodb.server.coordinator.meta.service.DingoMetaService;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;

import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableAdaptor extends TestBase {

    //@Test
    public void test01Create() {
        TableDefinition definition = new TableDefinition("TestTableAdaptor_testCreate");
        definition.setColumns(Arrays.asList(
            ColumnDefinition.builder().name("col1").type(SqlTypeName.INTEGER).build(),
            ColumnDefinition.builder().name("col2").type(SqlTypeName.VARCHAR).precision(32).build(),
            ColumnDefinition.builder().name("col3").type(SqlTypeName.INTEGER).build()
        ));
        TableAdaptor tableAdaptor = getMetaAdaptor(Table.class);
        tableAdaptor.create(DingoMetaService.DINGO_ID, definition);

        assertThat(tableAdaptor.get(definition.getName())).isEqualTo(definition);
        CommonId tableId = tableAdaptor.getTableId(definition.getName());
        List<TablePart> parts = getMetaAdaptor(TablePart.class)
            .getByDomain(tableId.seqContent());
        assertThat(parts).hasSize(1);
        TablePart tablePart = parts.get(0);
        assertThat(tablePart.getTable()).isEqualTo(tableId);
        assertThat(tablePart.getSchema()).isEqualTo(DingoMetaService.DINGO_ID);
    }

}
