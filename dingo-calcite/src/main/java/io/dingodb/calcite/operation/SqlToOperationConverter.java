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

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.grammar.ddl.SqlShowDatabases;
import io.dingodb.calcite.grammar.ddl.SqlShowGrants;
import io.dingodb.calcite.grammar.ddl.SqlShowTables;
import io.dingodb.calcite.grammar.ddl.SqlShowWarnings;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;
import java.util.Optional;

public class SqlToOperationConverter {

    public static Optional<Operation> convert(SqlNode sqlNode, Connection connection, DingoParserContext context) {
        if (sqlNode instanceof SqlShowWarnings) {
            return Optional.of(new ShowWarningsOperation());
        } else if (sqlNode instanceof SqlShowGrants) {
            return Optional.of(new ShowGrantsOperation(sqlNode));
        } else if (sqlNode instanceof SqlShowDatabases) {
            return Optional.of(new ShowDatabaseOperation(connection));
        } else if (sqlNode instanceof SqlShowTables) {
            return Optional.of(new ShowTableOperation(context.getUsedSchema().getName(), connection));
        } else if (sqlNode instanceof SqlSelect) {
            SqlNodeList sqlNodes = ((SqlSelect) sqlNode).getSelectList();
            SqlNode selectItem1 = sqlNodes.get(0);
            if (selectItem1 instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) selectItem1;
                String operatorName = sqlBasicCall.getOperator().getName();
                if (operatorName.equalsIgnoreCase("database")) {
                   return Optional.of(new ShowCurrentDatabase(context));
                }
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }
}
