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
import io.dingodb.calcite.grammar.ddl.SqlAlterUser;
import io.dingodb.calcite.grammar.ddl.SqlCreateIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropIndex;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.calcite.grammar.dql.SqlDesc;
import io.dingodb.calcite.grammar.dql.SqlShowCreateTable;
import io.dingodb.calcite.grammar.dql.SqlShowCreateUser;
import io.dingodb.calcite.grammar.dql.SqlShowFullTables;
import io.dingodb.calcite.grammar.dql.SqlShowGrants;
import io.dingodb.calcite.grammar.dql.SqlShowProcessList;
import io.dingodb.calcite.grammar.dql.SqlShowTableDistribution;
import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.privilege.DingoSqlAccessEnum;
import io.dingodb.verify.privilege.PrivilegeVerify;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.ddl.SqlDropTable;

import java.sql.SQLClientInfoException;
import java.util.ArrayList;
import java.util.List;

import static io.dingodb.calcite.runtime.DingoResource.DINGO_RESOURCE;

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
        } else if (sqlNode instanceof SqlCreateUser) {
            accessTypes.add(DingoSqlAccessEnum.CREATE_USER);
        } else if (sqlNode instanceof SqlGrant) {
            accessTypes.add(DingoSqlAccessEnum.GRANT);
            SqlGrant sqlGrant = (SqlGrant) sqlNode;
            if ("*".equals(sqlGrant.schema)) {
                schemaName = null;
            } else {
                schemaName = sqlGrant.schema;
            }
            if ("*".equals(sqlGrant.table)) {
                tableName = null;
            } else {
                tableName = sqlGrant.table;
            }
            schemaTables = new String[]{schemaName, tableName};
        } else if (sqlNode instanceof SqlFlushPrivileges) {
            accessTypes.add(DingoSqlAccessEnum.RELOAD);
        } else if (sqlNode instanceof SqlSetPassword) {
            SqlSetPassword sqlSetPassword = (SqlSetPassword) sqlNode;
            // allowed user modify self password
            if (user.equals(sqlSetPassword.user) && host.equals(sqlSetPassword.host)) {
                try {
                    connection.setClientInfo("@password_reset", "true");
                } catch (SQLClientInfoException e) {
                    throw new RuntimeException(e);
                }
                return;
            } else if ("root".equals(sqlSetPassword.user) && "%".equals(sqlSetPassword.host)) {
                throw new DingoSqlException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
            accessTypes.add(DingoSqlAccessEnum.UPDATE);
            schemaTables = new String[] {"mysql", "user"};
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
            if (!PrivilegeVerify.verify(user, host, schema, null, "getTables")) {
                throw new RuntimeException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
        } else if (sqlNode instanceof SqlAlterUser) {
            SqlAlterUser sqlAlterUser = (SqlAlterUser) sqlNode;
            // allow user modify self password
            if (user.equals(sqlAlterUser.user) && host.equals(sqlAlterUser.host)) {
                if (sqlAlterUser.password != null) {
                    try {
                        connection.setClientInfo("@password_reset", "true");
                    } catch (SQLClientInfoException e) {
                        throw new RuntimeException(e);
                    }
                }
                return;
            } else if ("root".equals(sqlAlterUser.user) && "%".equals(sqlAlterUser.host)) {
                throw new DingoSqlException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
            accessTypes.add(DingoSqlAccessEnum.CREATE_USER);
        } else if (sqlNode instanceof SqlShowGrants) {
            SqlShowGrants sqlShowGrants = (SqlShowGrants) sqlNode;
            if (user.equals(sqlShowGrants.user) && host.equals(sqlShowGrants.host)) {
                 return;
            }
            accessTypes.add(DingoSqlAccessEnum.SELECT);
            schemaTables = new String[] {"mysql", ""};
        } else if (sqlNode instanceof SqlShowCreateTable) {
            SqlShowCreateTable showCreateTable = (SqlShowCreateTable) sqlNode;
            if (showCreateTable.schemaName == null) {
                showCreateTable.schemaName = connection.getContext().getDefaultSchemaName();
            }
            if (!PrivilegeVerify.verify(user, host, showCreateTable.schemaName,
                showCreateTable.tableName, "getTables")) {
                throw new RuntimeException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
        } else if (sqlNode instanceof SqlDesc) {
            SqlDesc sqlDesc = (SqlDesc) sqlNode;
            if (sqlDesc.schemaName == null) {
                sqlDesc.schemaName = connection.getContext().getDefaultSchemaName();
            }
            if (!PrivilegeVerify.verify(user, host, sqlDesc.schemaName,
                sqlDesc.tableName, "getTables")) {
                throw new RuntimeException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
        } else if (sqlNode instanceof SqlShowCreateUser) {
            SqlShowCreateUser showCreateUser = (SqlShowCreateUser) sqlNode;
            if (user.equals(showCreateUser.userName) && host.equals(showCreateUser.host)) {
                return;
            }
            accessTypes.add(DingoSqlAccessEnum.SELECT);
            schemaTables = new String[] {"mysql", "user"};
        } else if (sqlNode instanceof SqlShowTableDistribution) {
            SqlShowTableDistribution sqlShowTableDistribution = (SqlShowTableDistribution) sqlNode;
            if (sqlShowTableDistribution.schemaName == null) {
                sqlShowTableDistribution.schemaName = connection.getContext().getDefaultSchemaName();
            }
            if (!PrivilegeVerify.verify(user, host, sqlShowTableDistribution.schemaName,
                sqlShowTableDistribution.tableName, "getTables")) {
                throw new RuntimeException(String.format("Access denied for user '%s'@'%s'", user, host));
            }
        } else if (sqlNode instanceof SqlLoadData) {
            SqlLoadData sqlLoadData = (SqlLoadData) sqlNode;
            schemaTables = initSchemaTable(sqlLoadData.getTableId().names, connection);
            accessTypes.add(DingoSqlAccessEnum.INSERT);
        } else if (sqlNode instanceof SqlSetOption) {
            SqlSetOption setOption = (SqlSetOption) sqlNode;
            if ("SYSTEM".equalsIgnoreCase(setOption.getScope())) {
                accessTypes.add(DingoSqlAccessEnum.SUPER);
            }
        } else if (sqlNode instanceof SqlShowProcessList) {
            accessTypes.add(DingoSqlAccessEnum.PROCESS);
        }

        if (schemaTables != null) {
            schemaName = schemaTables[0];
            tableName = schemaTables[1];
        }
        if (!PrivilegeVerify.verifyDuplicate(user, host, schemaName, tableName,
            accessTypes)) {
            if (sqlNode instanceof SqlShowProcessList) {
                SqlShowProcessList showProcessList = (SqlShowProcessList) sqlNode;
                showProcessList.setProcessPrivilege(false);
            } else {
                throw throwable(accessTypes, sqlNode, user, host, tableName);
            }
        }
    }

    public static DingoSqlException throwable(List<DingoSqlAccessEnum> accessTypes, SqlNode sqlNode,
                                              String user, String host,
                                              String tableName) {
        if (accessTypes.contains(DingoSqlAccessEnum.CREATE) || accessTypes.contains(DingoSqlAccessEnum.DROP)) {
            return DINGO_RESOURCE.operatorDenied(accessTypes.get(0).getAccessType(), user, host, tableName).ex();
        } else if (accessTypes.contains(DingoSqlAccessEnum.CREATE_USER) || sqlNode instanceof SqlDropUser) {
            return DINGO_RESOURCE.createUserDenied().ex();
        } else {
            return new DingoSqlException(String.format("Access denied for user '%s'@'%s'", user, host));
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
        // todo: current version, ignore name case
        CalciteSchema schema = connection.getContext().getRootSchema().getSubSchema(schemaName, false);
//        if (schema == null) {
//            throw new RuntimeException("Schema not found: " + schemaName);
//        }

        return new String[] {schemaName, tableName};
    }

}
