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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.grammar.dql.SqlNextAutoIncrement;
import io.dingodb.meta.MetaService;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowNextAutoIncrementOperation implements QueryOperation {

    private String schemaName;

    private String tableName;

    private MetaService metaService;

    public ShowNextAutoIncrementOperation(SqlNode sqlNode) {
        SqlNextAutoIncrement nextAutoIncrement = (SqlNextAutoIncrement) sqlNode;
        this.schemaName = nextAutoIncrement.schemaName;
        this.tableName = nextAutoIncrement.tableName;
        this.metaService = MetaService.root().getSubMetaService(schemaName);
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> tuples = new ArrayList<>();
        Long nextAutoIncrement = metaService.getNextAutoIncrement(metaService.getTable(tableName).getTableId());
        Object[] values = new Object[]{nextAutoIncrement};
        tuples.add(values);
        return tuples.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("NEXT_AUTO_INCREMENT");
        return columns;
    }

}
