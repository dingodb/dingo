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

import com.google.common.collect.ImmutableList;
import io.dingodb.common.CommonId;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TransactionService;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class LockTableOperation implements DdlOperation {

    private final String usedSchemaName;

    private final Connection connection;

    private final List<SqlIdentifier> tableList;

    public LockTableOperation(Connection connection, List<SqlIdentifier> tableList, String usedSchemaName) {
        this.connection = connection;
        this.tableList = tableList;
        this.usedSchemaName = usedSchemaName;
    }

    @Override
    public void execute() {
        List<CommonId> tables = new ArrayList<>(tableList.size());
        for (SqlIdentifier sqlIdentifier : tableList) {
            ImmutableList<String> names = sqlIdentifier.names;
            MetaService metaService = MetaService.root();
            String tableName = names.get(0);
            if (names.size() > 1) {
                metaService = metaService.getSubMetaService(names.get(0));
                tableName = names.get(1);
            } else {
                metaService = metaService.getSubMetaService(usedSchemaName);
            }
            tables.add(Parameters.nonNull(metaService.getTable(tableName), "table not found").getTableId());
        }
        TransactionService.getDefault().lockTable(connection, tables, LockType.TABLE);
    }
}
