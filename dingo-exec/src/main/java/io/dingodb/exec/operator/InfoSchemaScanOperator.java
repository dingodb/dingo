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

package io.dingodb.exec.operator;

import io.dingodb.cluster.ClusterService;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.StmtSummaryMap;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.InfoSchemaScanParam;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Partition;
import io.dingodb.meta.entity.Table;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.transaction.api.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class InfoSchemaScanOperator extends FilterProjectSourceOperator {
    public static final InfoSchemaScanOperator INSTANCE = new InfoSchemaScanOperator();

    private InfoSchemaScanOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        InfoSchemaScanParam param = vertex.getParam();
        String target = param.getTarget();
        switch (target.toUpperCase()) {
            case "GLOBAL_VARIABLES":
                return getGlobalVariables();
            case "TABLES":
                return getInformationTables();
            case "SCHEMATA":
                return getInformationSchemata();
            case "COLUMNS":
                return getInformationColumns();
            case "PARTITIONS":
                return getInformationPartitions();
            case "STATISTICS":
                return getInformationStatistics();
            case "VIEWS":
                return getView();
            case "USER_PRIVILEGES":
                return getUserPrivileges();
            case "SCHEMA_PRIVILEGES":
                return getSchemaPrivileges();
            case "TABLE_PRIVILEGES":
                return getTablePrivileges();
            case "EVENTS":
            case "TRIGGERS":
            case "ROUTINES":
            case "FILES":
            case "KEY_COLUMN_USAGE":
            case "COLUMN_STATISTICS":
            case "COLUMN_PRIVILEGES":
            case "COLLATIONS":
            case "PLUGINS":
            case "KEYWORDS":
            case "REFERENTIAL_CONSTRAINTS":
                return getEmpty();
            case "TABLE_CONSTRAINTS":
                return getInformationTableConstraints();
            case "STATEMENTS_SUMMARY":
                return StmtSummaryMap.iterator();
            case "DINGO_MDL_VIEW":
                return getMdlView();
            case "DINGO_TRX":
                return getTxnInfo();
            case "ENGINES":
                return mysqlEngineInfos();
            case "DINGO_ENGINES":
                return dingoEngineInfos();
            default:
                throw new RuntimeException("no source");
        }
    }

    private static Iterator<Object[]> getEmpty() {
        return new Iterator<Object[]>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object[] next() {
                return new Object[0];
            }
        };
    }

    private static Iterator<Object[]> getInformationColumns() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(schemaTables -> schemaTables.getTables()
                .values()
                .stream()
                .flatMap(td -> {
                    List<Object[]> colRes = new ArrayList<>();
                    for (int i = 0; i < td.getColumns().size(); i++) {
                        Column column = td.columns.get(i);
                        colRes.add(new Object[]{
                            "def",
                            schemaTables.getSchemaInfo().getName(),
                            td.getName(),
                            column.name,
                            // ordinal position
                            i + 1L,
                            // default value
                            column.defaultValueExpr,
                            // is null
                            column.isNullable() ? "YES" : "NO",
                            // type name
                            column.getSqlTypeName(),
                            (long) column.precision,
                            null,
                            null,
                            null,
                            null,
                            "utf8",
                            "utf8_bin",
                            column.getSqlTypeName(),
                            // is key
                            column.isPrimary() ? "PRI" : "",
                            "",
                            // privileges fix
                            "select,insert,update,references",
                            column.comment,
                            ""
                        });
                    }
                    return colRes.stream();
                })).iterator();
    }

    private static Iterator<Object[]> getInformationPartitions() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(schemaTables -> schemaTables.getTables()
                .values()
                .stream()
                .filter(table -> table.partitions != null && !table.getPartitions().isEmpty())
                .flatMap(table -> table.getPartitions()
                    .stream()
                    .map(partition -> getPartitionDetail(
                        schemaTables.getSchemaInfo().getName(), table, partition))))
            .iterator();
    }

    private static Object[] getPartitionDetail(String schemaName, Table td, Partition partition) {
        if (partition == null) {
            return new Object[]{};
        }
        String operand = null;
        if (partition != null && partition.getOperand() != null) {
            operand = Arrays.toString(partition.getOperand());
        }
        return new Object[]{
            "def",
            schemaName,
            td.getName(),
            // part name
            partition.getName(),
            // sub part name
            null,
            // part ordinal position
            null,
            // sub part ordinal position
            null,
            // part method
            null,
            // sub part method
            null,
            // part expr
            null,
            // sub part expr
            null,
            // part desc
            operand,
            // table rows
            null,
            // avg row length
            null,
            // data length
            null,
            // max data length
            null,
            // index length
            0L,
            // data free
            null,
            new Timestamp(td.getCreateTime()),
            td.getUpdateTime() == 0 ? null : new Timestamp(td.getUpdateTime()),
            // check time
            null,
            // check sum
            null,
            // part comment
            null,
            // node group
            null,
            // tablespace name
            null
        };
    }

    private static Iterator<Object[]> getGlobalVariables() {
        InfoSchemaService service = InfoSchemaService.root();
        assert service != null;
        Map<String, String> response = service.getGlobalVariables();
        List<Object[]> resList = response
            .entrySet()
            .stream()
            .map(e -> new Object[]{e.getKey(), e.getValue()})
            .collect(Collectors.toList());
        return resList.iterator();
    }

    private static Iterator<Object[]> getInformationSchemata() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .keySet()
            .stream()
            .map(service -> new Object[]{"def", service, "utf8", "utf8_bin", null})
            .iterator();
    }

    private static Iterator<Object[]> getInformationTables() {
        MetaService metaService = MetaService.root();
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap().values()
            .stream()
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                return tables.stream()
                    .map(td -> {
                        Timestamp updateTime = null;
                        if (td.getUpdateTime() > 0) {
                            updateTime = new Timestamp(td.getUpdateTime());
                        }
                        String createOptions = "";
                        if (!td.getProperties().isEmpty()) {
                            createOptions = td.getProperties().toString();
                        }
                        boolean hasInc = td.getColumns().stream().anyMatch(Column::isAutoIncrement);
                        try {
                            return new Object[]{"def",
                                e.getSchemaInfo().getName(),
                                td.getName(),
                                td.tableType,
                                td.getEngine(),
                                td.getVersion(),
                                td.getRowFormat(),
                                // table rows
                                null,
                                // avg row length
                                0L,
                                // data length
                                0L,
                                // max data length
                                0L,
                                // index length
                                0L,
                                // data free
                                null,
                                hasInc ? metaService.getLastId(td.tableId) : null,
                                new Timestamp(td.getCreateTime()),
                                updateTime,
                                null,
                                td.getCollate(),
                                null,
                                createOptions,
                                td.getComment()
                            };
                        } catch (Exception e1) {
                            LogUtils.error(log, e1.getMessage(), e1);
                            return null;
                        }
                    }).filter(Objects::nonNull)
                    .collect(Collectors.toList()).stream();
            })
            .iterator();
    }

    private static Iterator<Object[]> getInformationTableConstraints() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                return tables.stream()
                    .map(td -> new Object[]{"def",
                        e.getSchemaInfo().getName(),
                        "PRIMARY",
                        e.getSchemaInfo().getName(),
                        td.getName(),
                        "PRIMARY KEY"
                    })
                    .collect(Collectors.toList()).stream();
            }).iterator();
    }

    private static Iterator<Object[]> getInformationStatistics() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                List<Object[]> priKeyList = tables.stream()
                    .flatMap(table -> table.getColumns().stream().filter(Column::isPrimary).map(
                        column -> new Object[]{
                            "def",
                            e.getSchemaInfo().getName(),
                            table.name,
                            0,
                            e.getSchemaInfo().getName(),
                            "PRIMARY",
                            column.primaryKeyIndex,
                            column.name,
                            "A",
                            0,
                            null,
                            null,
                            column.isNullable() ? "YES" : "NO",
                            table.getEngine(),
                            column.getComment(),
                            ""
                        }
                    )).collect(Collectors.toList());
                List<Object[]> indexColList = tables.stream().flatMap(table -> table.getIndexes().stream()
                    .flatMap(index -> index.getColumns().stream().filter(Column::isPrimary).map(
                        column -> new Object[]{
                            "def",
                            e.getSchemaInfo().getName(),
                            index.name,
                            index.isUnique() ? 0 : 1,
                            e.getSchemaInfo().getName(),
                            index.getName(),
                            column.primaryKeyIndex,
                            column.name,
                            "A",
                            0,
                            null,
                            null,
                            column.isNullable() ? "YES" : "NO",
                            index.getEngine(),
                            column.getComment(),
                            ""
                        }
                    ))).collect(Collectors.toList());
                priKeyList.addAll(indexColList);
                return priKeyList.stream();
            }).iterator();
    }

    private static Iterator<Object[]> getMdlView() {
        return TransactionService.getDefault().getMdlInfo();
    }

    public static Iterator<Object[]> getView() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap().values()
            .stream()
            .filter(schemaTables ->
                !schemaTables.getSchemaInfo().getName().equalsIgnoreCase("INFORMATION_SCHEMA"))
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                return tables.stream()
                    .filter(td -> td.getTableType().equalsIgnoreCase("VIEW"))
                    .map(td -> {
                        String checkOpt = td.getProperties()
                            .getProperty("check_option", "").toUpperCase();
                        String isUpdaTable = "NO";
                        String user = td.getProperties().getProperty("user", "");
                        String host = td.getProperties().getProperty("host", "");
                        String definer = user + "@" + host;
                        String security = td.getProperties().getProperty("security_type");
                        String character = "utf8";
                        String collate = "utf8mb4_bin";
                        try {
                            return new Object[]{"def",
                                e.getSchemaInfo().getName(),
                                td.getName(),
                                td.createSql,
                                checkOpt,
                                isUpdaTable,
                                definer,
                                security,
                                character,
                                collate
                            };
                        } catch (Exception e1) {
                            LogUtils.error(log, e1.getMessage(), e1);
                            return null;
                        }
                    }).filter(Objects::nonNull)
                    .collect(Collectors.toList()).stream();
            })
            .iterator();
    }

    /**
    * Api to get remote txn informations.
    */
    public interface Api {
        @ApiDeclaration
        default List<Object[]> txnInfos() {
            return new ArrayList<>();
        }

        @ApiDeclaration
        default List<Object[]> getTxnInfos() {
            List<Object[]> results = new ArrayList<>();
            Iterator<Object[]> iterator = TransactionService.getDefault().getTxnInfo();
            while(iterator.hasNext()) {
                results.add(iterator.next());
            }
            return results;
        }
    }

    /**
     * The function is triggered by selecting dingo_trx table to fetch cluster transaction infos.
     * @return The transaction informations in cluster.
     */
    private static Iterator<Object[]> getTxnInfo() {
        List<Object[]> result = new ArrayList<>();

        //get remote txn infos.
        ClusterService.getDefault().getComputingLocations().stream()
            .filter($ -> !$.equals(DingoConfiguration.location()))
            .map($ -> ApiRegistry.getDefault().proxy(InfoSchemaScanOperator.Api.class, $))
            .map(InfoSchemaScanOperator.Api::getTxnInfos)
            .forEach(result::addAll);

        //get local txn infos.
        Iterator<Object[]> iterator = TransactionService.getDefault().getTxnInfo();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        return result.stream().iterator();
    }

    private static Iterator<Object[]> getUserPrivileges() {
        Session session = SessionUtil.INSTANCE.getSession();
        Map<Integer, String> privilegeMap = new HashMap<>();
        privilegeMap.put(2, "SELECT");
        privilegeMap.put(3, "INSERT");
        privilegeMap.put(4, "UPDATE");
        privilegeMap.put(5, "DELETE");
        privilegeMap.put(6, "CREATE");
        privilegeMap.put(7, "DROP");
        privilegeMap.put(8, "RELOAD");
        privilegeMap.put(9, "SHUTDOWN");
        privilegeMap.put(10, "PROCESS");
        privilegeMap.put(11, "FILE");
        //privilegeMap.put(12, "GRANT");
        privilegeMap.put(13, "REFERENCES");
        privilegeMap.put(14, "INDEX");
        privilegeMap.put(15, "ALTER");
        privilegeMap.put(16, "SHOW DATABASE");
        privilegeMap.put(17, "SUPER");
        privilegeMap.put(18, "CREATE TEMPORARY TABLES");
        privilegeMap.put(19, "LOCK TABLES");
        privilegeMap.put(20, "EXECUTE");
        privilegeMap.put(21, "REPLICATION SLAVE");
        privilegeMap.put(22, "REPLICATION CLIENT");
        privilegeMap.put(23, "CREATE VIEW");
        privilegeMap.put(24, "SHOW VIEW");
        privilegeMap.put(25, "CREATE ROUTINE");
        privilegeMap.put(26, "ALTER ROUTINE");
        privilegeMap.put(27, "CREATE USER");
        privilegeMap.put(28, "EVENT");
        privilegeMap.put(29, "TRIGGER");
        privilegeMap.put(30, "CREATE TABLESPACE");
        try {
            String sql = "select * from mysql.user";
            List<Object[]> users = session.executeQuery(sql);
            return users.stream().flatMap(userRow -> {
                List<Object[]> userPrivilegeList = new ArrayList<>();
                String grantee = "'" + userRow[1] + "'@'" + userRow[0] + "'";
                String isGrantee =  userRow[12] != null
                    ? userRow[12].toString() : "N";

                privilegeMap.forEach((key, value) -> {
                    String privilege = userRow[key] != null
                        ? userRow[key].toString() : "N";
                    if ("Y".equalsIgnoreCase(privilege)) {
                        Object[] userPrivileges = new Object[4];
                        userPrivileges[0] = grantee;
                        userPrivileges[1] = "def";
                        userPrivileges[2] = value;
                        userPrivileges[3] = "Y".equalsIgnoreCase(isGrantee) ? "YES" : "NO";
                        userPrivilegeList.add(userPrivileges);
                    }
                });

                return userPrivilegeList.stream();
            }).iterator();
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        return getEmpty();
    }

    private static Iterator<Object[]> getSchemaPrivileges() {
        Session session = SessionUtil.INSTANCE.getSession();
        Map<Integer, String> privilegeMap = new HashMap<>();
        privilegeMap.put(3, "SELECT");
        privilegeMap.put(4, "INSERT");
        privilegeMap.put(5, "UPDATE");
        privilegeMap.put(6, "DELETE");
        privilegeMap.put(7, "CREATE");
        privilegeMap.put(8, "DROP");
        privilegeMap.put(9, "GRANT");
        privilegeMap.put(10, "REFERENCES");
        privilegeMap.put(11, "INDEX");
        privilegeMap.put(12, "ALTER");
        privilegeMap.put(13, "CREATE TEMPORARY TABLES");
        privilegeMap.put(14, "LOCK TABLES");
        privilegeMap.put(15, "CREATE VIEW");
        privilegeMap.put(16, "SHOW VIEW");
        privilegeMap.put(17, "CREATE ROUTINE");
        privilegeMap.put(18, "ALTER ROUTINE");
        privilegeMap.put(19, "EXECUTE");
        privilegeMap.put(20, "EVENT");
        privilegeMap.put(21, "TRIGGER");
        try {
            String sql = "select * from mysql.db";
            List<Object[]> users = session.executeQuery(sql);
            return users.stream().flatMap(userRow -> {
                List<Object[]> privilegeList = new ArrayList<>();
                String grantee = "'" + userRow[1] + "'@'" + userRow[0] + "'";
                String isGrantee =  userRow[9] != null
                    ? userRow[9].toString() : "N";
                String schema = userRow[2] != null ? userRow[2].toString() : "";

                privilegeMap.forEach((key, value) -> {
                    String privilege = userRow[key] != null
                        ? userRow[key].toString() : "N";
                    if ("Y".equalsIgnoreCase(privilege)) {
                        Object[] schemaPrivileges = new Object[5];
                        schemaPrivileges[0] = grantee;
                        schemaPrivileges[1] = "def";
                        schemaPrivileges[2] = schema;
                        schemaPrivileges[3] = value;
                        schemaPrivileges[4] = "Y".equalsIgnoreCase(isGrantee) ? "YES" : "NO";
                        privilegeList.add(schemaPrivileges);
                    }
                });

                return privilegeList.stream();
            }).iterator();
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        return getEmpty();
    }

    private static Iterator<Object[]> getTablePrivileges() {
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            String sql = "select * from mysql.tables_priv";
            List<Object[]> users = session.executeQuery(sql);
            return users.stream().flatMap(userRow -> {
                List<Object[]> privilegeList = new ArrayList<>();
                String grantee = "'" + userRow[1] + "'@'" + userRow[0] + "'";
                String schema = userRow[2] != null ? userRow[2].toString() : "";
                String table =  userRow[3] != null ? userRow[3].toString() : "";
                String tablePrivStr =  userRow[6] != null ? userRow[6].toString() : "";
                String[] tablePriv = tablePrivStr.split(",");
                boolean isGrantee = tablePrivStr.contains("Grant");

                for (String privilege : tablePriv) {
                    Object[] tablePrivileges = new Object[6];
                    tablePrivileges[0] = grantee;
                    tablePrivileges[1] = "def";
                    tablePrivileges[2] = schema;
                    tablePrivileges[3] = table;
                    tablePrivileges[4] = privilege.toUpperCase();
                    tablePrivileges[5] = isGrantee ? "YES" : "NO";
                    privilegeList.add(tablePrivileges);
                }

                return privilegeList.stream();
            }).iterator();
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        return getEmpty();
    }

    private static Iterator<Object[]> mysqlEngineInfos() {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{"ndbcluster", "Clustered, fault-tolerant tables", null, "NO", null, null});

        return result.stream().iterator();
    }

    private static Iterator<Object[]> dingoEngineInfos() {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{"TXN_LSM", "LSM based engine with transactions.", "NO", "YES", "YES", "YES"});
        result.add(new Object[]{"TXN_BTREE", "BTREE based engine with transactions.", "NO", "YES", "YES", "YES"});
        result.add(new Object[]{"LSM", "LSM based engine without transactions.", "NO", "YES", "NO", "NO"});
        result.add(new Object[]{"BTREE", "BTREE based engine without transactions.", "NO", "YES", "NO", "NO"});
        return result.stream().iterator();
    }
}
