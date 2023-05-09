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

package io.dingodb.driver;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropIndex;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.calcite.grammar.dql.SqlShowFullTables;
import io.dingodb.common.privilege.DingoSqlAccessEnum;
import io.dingodb.verify.privilege.PrivilegeVerify;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropTable;

import java.util.ArrayList;
import java.util.List;

public class DingoDdlVerify {

    private DingoDdlVerify() {
    }

    public static void verify(SqlNode sqlNode, DingoConnection connection) {
        if (!PrivilegeVerify.isVerify) {
            return;
        }
        String user = connection.getContext().getOption("user");
        String host = connection.getContext().getOption("host");
        List<DingoSqlAccessEnum> accessTypes = new ArrayList<>();
        String schemaName = null;
        String tableName = null;
        String[] schemaTables = null;
        if (sqlNode instanceof DingoSqlCreateTable) {
            accessTypes.add(DingoSqlAccessEnum.CREATE);
            DingoSqlCreateTable sqlCreateTable = (DingoSqlCreateTable) sqlNode;
            if (sqlCreateTable.columnList != null) {
                long indexCount = sqlCreateTable.columnList.stream()
                    .filter(col -> col.getKind() == SqlKind.CREATE_INDEX).count();
                if (indexCount > 0) {
                    accessTypes.add(DingoSqlAccessEnum.INDEX);
                }
            }
            schemaTables = initSchemaTable(sqlCreateTable.name.names, connection);
        } else if (sqlNode instanceof SqlDropUser) {
            accessTypes.add(DingoSqlAccessEnum.DROP);
        } else if (sqlNode instanceof SqlDropTable) {
            accessTypes.add(DingoSqlAccessEnum.DROP);
            SqlDropTable sqlDropTable = (SqlDropTable) sqlNode;
            schemaTables = initSchemaTable(sqlDropTable.name.names, connection);
        } else if (sqlNode instanceof SqlCreateUser || sqlNode instanceof SqlGrant) {
            accessTypes.add(DingoSqlAccessEnum.CREATE_USER);
        } else if (sqlNode instanceof SqlFlushPrivileges) {
            accessTypes.add(DingoSqlAccessEnum.RELOAD);
        } else if (sqlNode instanceof SqlSetPassword) {
            if (!"root".equals(user)) {
                throw new RuntimeException("Access denied");
            }
        } else if (sqlNode instanceof SqlTruncate) {
            accessTypes.add(DingoSqlAccessEnum.DROP);
            accessTypes.add(DingoSqlAccessEnum.CREATE);
            SqlTruncate sqlTruncate = (SqlTruncate) sqlNode;
            schemaTables = initSchemaTable(sqlTruncate.id.names, connection);
        } else if (sqlNode instanceof SqlAlterAddIndex) {
            accessTypes.add(DingoSqlAccessEnum.ALTER);
            accessTypes.add(DingoSqlAccessEnum.INDEX);
            SqlAlterAddIndex sqlAlterTable = (SqlAlterAddIndex) sqlNode;
            schemaTables = initSchemaTable(sqlAlterTable.table.names, connection);
        } else if (sqlNode instanceof SqlCreateIndex) {
            accessTypes.add(DingoSqlAccessEnum.INDEX);
            SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) sqlNode;
            schemaTables = initSchemaTable(sqlCreateIndex.table.names, connection);
        } else if (sqlNode instanceof SqlDropIndex) {
            accessTypes.add(DingoSqlAccessEnum.INDEX);
            SqlDropIndex sqlDropIndex = (SqlDropIndex) sqlNode;
            schemaTables = initSchemaTable(sqlDropIndex.table.names, connection);
        } else if (sqlNode instanceof SqlShowFullTables) {
            SqlShowFullTables showFullTables = (SqlShowFullTables) sqlNode;
            String schema = showFullTables.schema;
            if (schema == null) {
                schema = connection.getContext().getDefaultSchemaName();
            }
            if (!PrivilegeVerify.verify(user, host, schema, null)) {
                throw new RuntimeException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
        }

        if (schemaTables != null) {
            schemaName = schemaTables[0];
            tableName = schemaTables[1];
        }
        if (!PrivilegeVerify.verifyDuplicate(user, host, schemaName, tableName,
            accessTypes)) {
            throw new RuntimeException(String.format("Access denied for user '%s'@'%s'", user, host));
        }
    }

    public static String[] initSchemaTable(ImmutableList<String> names,
                                           DingoConnection connection
                                           ) {
        String schemaName;
        String tableName;
        if (names.size() == 1) {
            tableName = names.get(0).toUpperCase();
            if (connection.getContext().getUsedSchema() == null) {
                schemaName = connection.getContext().getDefaultSchemaName();
            } else {
                schemaName = connection.getContext().getUsedSchema().getName();
            }
        } else {
            schemaName = names.get(0);
            tableName = names.get(1).toUpperCase();
        }
        if (schemaName != null) {
            schemaName = schemaName.toUpperCase();
        }
        CalciteSchema schema = connection.getContext().getRootSchema().getSubSchema(schemaName, true);
        if (schema == null) {
            throw new RuntimeException("Schema not found: " + schemaName);
        }

        return new String[] {schemaName, tableName};
    }

}
