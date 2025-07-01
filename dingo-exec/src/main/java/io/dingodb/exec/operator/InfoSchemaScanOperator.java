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
import io.dingodb.verify.privilege.PrivilegeVerify;
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
        String user = param.getUser();
        String host = param.getHost();
        String target = param.getTarget();
        switch (target.toUpperCase()) {
            case "GLOBAL_VARIABLES":
                return getGlobalVariables();
            case "TABLES":
                return getInformationTables(user, host);
            case "SCHEMATA":
                return getInformationSchemata(user, host);
            case "COLUMNS":
                return getInformationColumns(user, host);
            case "PARTITIONS":
                return getInformationPartitions(user, host);
            case "STATISTICS":
                return getInformationStatistics(user, host);
            case "VIEWS":
                return getView(user, host);
            case "USER_PRIVILEGES":
                return getUserPrivileges(user, host);
            case "SCHEMA_PRIVILEGES":
                return getSchemaPrivileges(user, host);
            case "TABLE_PRIVILEGES":
                return getTablePrivileges(user, host);
            case "KEYWORDS":
                return getKeywords();
            case "EVENTS":
            case "TRIGGERS":
            case "ROUTINES":
            case "FILES":
            case "KEY_COLUMN_USAGE":
            case "COLUMN_STATISTICS":
            case "COLUMN_PRIVILEGES":
            case "COLLATIONS":
            case "PLUGINS":
            case "REFERENTIAL_CONSTRAINTS":
                return getEmpty();
            case "TABLE_CONSTRAINTS":
                return getInformationTableConstraints(user, host);
            case "STATEMENTS_SUMMARY":
                return StmtSummaryMap.iterator(user, host);
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

    private static Iterator<Object[]> getInformationColumns(String user, String host) {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(schemaTables -> schemaTables.getTables()
                .values()
                .stream()
                .filter(td -> PrivilegeVerify.verify(user, host, schemaTables.getSchemaInfo().getName(),
                    td.getName(), "dataPrivilege"))
                .flatMap(td -> {
                    List<Object[]> colRes = new ArrayList<>();
                    for (int i = 0; i < td.getColumns().size(); i++) {
                        Column column = td.columns.get(i);
                        if (column.state == 2) {
                            continue;
                        }
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

    private static Iterator<Object[]> getInformationPartitions(String user, String host) {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(schemaTables -> schemaTables.getTables()
                .values()
                .stream()
                .filter(table -> {
                    boolean authed = PrivilegeVerify.verify(user, host, schemaTables.getSchemaInfo().getName(),
                        table.getName(), "dataPrivilege");
                    return table.partitions != null && !table.getPartitions().isEmpty() && authed;
                })
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

    private static Iterator<Object[]> getInformationSchemata(String user, String host) {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .keySet()
            .stream()
            .filter(schemaName -> PrivilegeVerify.verify(user, host, schemaName, null, "dataPrivilege"))
            .map(service -> new Object[]{"def", service, "utf8", "utf8_bin", null})
            .iterator();
    }

    private static Iterator<Object[]> getInformationTables(String user, String host) {
        MetaService metaService = MetaService.root();
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap().values()
            .stream()
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                return tables.stream()
                    .map(td -> {
                        boolean authed = PrivilegeVerify.verify(
                            user, host, e.getSchemaInfo().getName(), td.getName(), "dataPrivilege");
                        if (!authed) {
                            return null;
                        }
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

    private static Iterator<Object[]> getInformationTableConstraints(String user, String host) {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                return tables.stream()
                    .filter(td -> PrivilegeVerify.verify(user, host, e.getSchemaInfo().getName(),
                        td.getName(), "dataPrivilege"))
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

    private static Iterator<Object[]> getInformationStatistics(String user, String host) {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .values()
            .stream()
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                List<Object[]> priKeyList = tables.stream()
                    .filter(table -> PrivilegeVerify.verify(user, host, e.getSchemaInfo().getName(),
                        table.getName(), "dataPrivilege"))
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
                List<Object[]> indexColList = tables.stream()
                    .filter(table -> PrivilegeVerify.verify(user, host, e.getSchemaInfo().getName(),
                        table.getName(), "dataPrivilege")).flatMap(table -> table.getIndexes().stream()
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

    public static Iterator<Object[]> getView(String userVer, String hostVerf) {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap().values()
            .stream()
            .filter(schemaTables ->
                !schemaTables.getSchemaInfo().getName().equalsIgnoreCase("INFORMATION_SCHEMA"))
            .flatMap(e -> {
                Collection<Table> tables = e.getTables().values();
                return tables.stream()
                    .filter(td -> {
                        boolean authed = PrivilegeVerify.verify(userVer, hostVerf, e.getSchemaInfo().getName(), td.getName(), "dataPrivilege");
                        return td.getTableType().equalsIgnoreCase("VIEW") && authed;
                    })
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
                        String sql = td.properties.getOrDefault("originSql", td.createSql).toString();
                        try {
                            return new Object[]{"def",
                                e.getSchemaInfo().getName(),
                                td.getName(),
                                sql,
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

    private static Iterator<Object[]> getUserPrivileges(String user, String host) {
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
            return users.stream().filter(userRow -> {
                if ("root".equals(user)) {
                    return true;
                }
                String user1 = userRow[1].toString();
                String host1 = userRow[0].toString();
                return user.equals(user1) && host.equals(host1);
            }).flatMap(userRow -> {
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

    private static Iterator<Object[]> getSchemaPrivileges(String user, String host) {
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
            return users.stream().filter(userRow -> {
                if ("root".equals(user)) {
                    return true;
                }
                String user1 = userRow[1].toString();
                String host1 = userRow[0].toString();
                return user.equals(user1) && host.equals(host1);
            }).flatMap(userRow -> {
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

    private static Iterator<Object[]> getTablePrivileges(String user, String host) {
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            String sql = "select * from mysql.tables_priv";
            List<Object[]> users = session.executeQuery(sql);
            return users.stream().filter(userRow -> {
                if ("root".equals(user)) {
                    return true;
                }
                String user1 = userRow[1].toString();
                String host1 = userRow[0].toString();
                return user.equals(user1) && host.equals(host1);
            }).flatMap(userRow -> {
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

    public static Iterator<Object[]> getKeywords() {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{"ABS", 1});
        result.add(new Object[]{"ALL", 1});
        result.add(new Object[]{"ALLOCATE", 1});
        result.add(new Object[]{"ALLOW", 1});
        result.add(new Object[]{"ALTER", 1});
        result.add(new Object[]{"AND", 1});
        result.add(new Object[]{"ANY", 1});
        result.add(new Object[]{"ARE", 1});
        result.add(new Object[]{"ARRAY", 1});
        result.add(new Object[]{"ARRAY_MAX_CARDINALITY", 1});
        result.add(new Object[]{"AS", 1});
        result.add(new Object[]{"ASENSITIVE", 1});
        result.add(new Object[]{"ASYMMETRIC", 1});
        result.add(new Object[]{"AT", 1});
        result.add(new Object[]{"ATOMIC", 1});
        result.add(new Object[]{"@", 1});
        result.add(new Object[]{"@@", 1});
        result.add(new Object[]{"AUTHORIZATION", 1});
        result.add(new Object[]{"AVG", 1});
        result.add(new Object[]{"BEGIN", 1});
        result.add(new Object[]{"BEGIN_FRAME", 1});
        result.add(new Object[]{"BEGIN_PARTITION", 1});
        result.add(new Object[]{"BETWEEN", 1});
        result.add(new Object[]{"BIGINT", 1});
        result.add(new Object[]{"BINARY", 1});
        result.add(new Object[]{"BIT", 1});
        result.add(new Object[]{"BOOLEAN", 1});
        result.add(new Object[]{"BOTH", 1});
        result.add(new Object[]{"BY", 1});
        result.add(new Object[]{"CALL", 1});
        result.add(new Object[]{"CALLED", 1});
        result.add(new Object[]{"CARDINALITY", 1});
        result.add(new Object[]{"CASCADED", 1});
        result.add(new Object[]{"CASE", 1});
        result.add(new Object[]{"CAST", 1});
        result.add(new Object[]{"CEIL", 1});
        result.add(new Object[]{"CEILING", 1});
        result.add(new Object[]{"CHAR", 1});
        result.add(new Object[]{"CHAR_LENGTH", 1});
        result.add(new Object[]{"CHARACTER", 1});
        result.add(new Object[]{"CHARACTER_LENGTH", 1});
        result.add(new Object[]{"CHECK", 1});
        result.add(new Object[]{"CLASSIFIER", 1});
        result.add(new Object[]{"CLOB", 1});
        result.add(new Object[]{"CLOSE", 1});
        result.add(new Object[]{"COALESCE", 1});
        result.add(new Object[]{"COLLATE", 1});
        result.add(new Object[]{"COLLECT", 1});
        result.add(new Object[]{"COLUMN", 1});
        result.add(new Object[]{"COMMIT", 1});
        result.add(new Object[]{"CONDITION", 1});
        result.add(new Object[]{"CONNECT", 1});
        result.add(new Object[]{"CONSTRAINT", 1});
        result.add(new Object[]{"CONTAINS", 1});
        result.add(new Object[]{"CONVERT", 1});
        result.add(new Object[]{"CORR", 1});
        result.add(new Object[]{"CORRESPONDING", 1});
        result.add(new Object[]{"COUNT", 1});
        result.add(new Object[]{"COVAR_POP", 1});
        result.add(new Object[]{"COVAR_SAMP", 1});
        result.add(new Object[]{"CREATE", 1});
        result.add(new Object[]{"CROSS", 1});
        result.add(new Object[]{"CUBE", 1});
        result.add(new Object[]{"CUME_DIST", 1});
        result.add(new Object[]{"CURRENT", 1});
        result.add(new Object[]{"CURRENT_CATALOG", 1});
        result.add(new Object[]{"CURRENT_DATE", 1});
        result.add(new Object[]{"CURRENT_DEFAULT_TRANSFORM_GROUP", 1});
        result.add(new Object[]{"CURRENT_PATH", 1});
        result.add(new Object[]{"CURRENT_ROLE", 1});
        result.add(new Object[]{"CURRENT_ROW", 1});
        result.add(new Object[]{"CURRENT_SCHEMA", 1});
        result.add(new Object[]{"CURRENT_TIME", 1});
        result.add(new Object[]{"CURRENT_TIMESTAMP", 1});
        result.add(new Object[]{"CURRENT_TRANSFORM_GROUP_FOR_TYPE", 1});
        result.add(new Object[]{"CURRENT_USER", 1});
        result.add(new Object[]{"CURSOR", 1});
        result.add(new Object[]{"CYCLE", 1});
        result.add(new Object[]{"DATETIME", 1});
        result.add(new Object[]{"DAY", 1});
        result.add(new Object[]{"DEALLOCATE", 1});
        result.add(new Object[]{"DEC", 1});
        result.add(new Object[]{"DECIMAL", 1});
        result.add(new Object[]{"DECLARE", 1});
        result.add(new Object[]{"DEFAULT", 1});
        result.add(new Object[]{"DEFINE", 1});
        result.add(new Object[]{"DELETE", 1});
        result.add(new Object[]{"DENSE_RANK", 1});
        result.add(new Object[]{"DEREF", 1});
        result.add(new Object[]{"DESCRIBE", 1});
        result.add(new Object[]{"DETERMINISTIC", 1});
        result.add(new Object[]{"DISALLOW", 1});
        result.add(new Object[]{"DISCONNECT", 1});
        result.add(new Object[]{"DISTINCT", 1});
        result.add(new Object[]{"DOT", 1});
        result.add(new Object[]{"DOUBLE", 1});
        result.add(new Object[]{"DUPLICATE", 1});
        result.add(new Object[]{"DROP", 1});
        result.add(new Object[]{"DYNAMIC", 1});
        result.add(new Object[]{"EACH", 1});
        result.add(new Object[]{"ELEMENT", 1});
        result.add(new Object[]{"ELSE", 1});
        result.add(new Object[]{"EMPTY", 1});
        result.add(new Object[]{"END", 1});
        result.add(new Object[]{"END-EXEC", 1});
        result.add(new Object[]{"END_FRAME", 1});
        result.add(new Object[]{"END_PARTITION", 1});
        result.add(new Object[]{"EQUALS", 1});
        result.add(new Object[]{"ESCAPE", 1});
        result.add(new Object[]{"EVERY", 1});
        result.add(new Object[]{"EXCEPT", 1});
        result.add(new Object[]{"EXEC", 1});
        result.add(new Object[]{"EXECUTE", 1});
        result.add(new Object[]{"EXISTS", 1});
        result.add(new Object[]{"EXP", 1});
        result.add(new Object[]{"EXPLAIN", 1});
        result.add(new Object[]{"EXTEND", 1});
        result.add(new Object[]{"EXTERNAL", 1});
        result.add(new Object[]{"EXTRACT", 1});
        result.add(new Object[]{"FALSE", 1});
        result.add(new Object[]{"FETCH", 1});
        result.add(new Object[]{"FILTER", 1});
        result.add(new Object[]{"FIRST_VALUE", 1});
        result.add(new Object[]{"FLOAT", 1});
        result.add(new Object[]{"FLOOR", 1});
        result.add(new Object[]{"FOR", 1});
        result.add(new Object[]{"FOREIGN", 1});
        result.add(new Object[]{"FRAME_ROW", 1});
        result.add(new Object[]{"FREE", 1});
        result.add(new Object[]{"FRIDAY", 1});
        result.add(new Object[]{"FROM", 1});
        result.add(new Object[]{"FULL", 1});
        result.add(new Object[]{"FUNCTION", 1});
        result.add(new Object[]{"FUSION", 1});
        result.add(new Object[]{"GET", 1});
        result.add(new Object[]{"GRANT", 1});
        result.add(new Object[]{"GROUP", 1});
        result.add(new Object[]{"GROUPING", 1});
        result.add(new Object[]{"GROUPS", 1});
        result.add(new Object[]{"HAVING", 1});
        result.add(new Object[]{"HOLD", 1});
        result.add(new Object[]{"HOUR", 1});
        result.add(new Object[]{"IDENTITY", 1});
        result.add(new Object[]{"IFNULL", 1});
        result.add(new Object[]{"IMPORT", 1});
        result.add(new Object[]{"IN", 1});
        result.add(new Object[]{"INDICATOR", 1});
        result.add(new Object[]{"INITIAL", 1});
        result.add(new Object[]{"INNER", 1});
        result.add(new Object[]{"INOUT", 1});
        result.add(new Object[]{"INSENSITIVE", 1});
        result.add(new Object[]{"INSERT", 1});
        result.add(new Object[]{"INT", 1});
        result.add(new Object[]{"INTEGER", 1});
        result.add(new Object[]{"INTERSECT", 1});
        result.add(new Object[]{"INTERSECTION", 1});
        result.add(new Object[]{"INTERVAL", 1});
        result.add(new Object[]{"INTO", 1});
        result.add(new Object[]{"IS", 1});
        result.add(new Object[]{"JOIN", 1});
        result.add(new Object[]{"JSON_ARRAY", 1});
        result.add(new Object[]{"JSON_ARRAYAGG", 1});
        result.add(new Object[]{"JSON_EXISTS", 1});
        result.add(new Object[]{"JSON_OBJECT", 1});
        result.add(new Object[]{"JSON_OBJECTAGG", 1});
        result.add(new Object[]{"JSON_QUERY", 1});
        result.add(new Object[]{"JSON_VALUE", 1});
        result.add(new Object[]{"KEY", 1});
        result.add(new Object[]{"LAG", 1});
        result.add(new Object[]{"LANGUAGE", 1});
        result.add(new Object[]{"LARGE", 1});
        result.add(new Object[]{"LAST_VALUE", 1});
        result.add(new Object[]{"LATERAL", 1});
        result.add(new Object[]{"LEAD", 1});
        result.add(new Object[]{"LEADING", 1});
        result.add(new Object[]{"LEFT", 1});
        result.add(new Object[]{"LIKE", 1});
        result.add(new Object[]{"LIKE BINARY", 1});
        result.add(new Object[]{"LIKE_REGEX", 1});
        result.add(new Object[]{"LIMIT", 1});
        result.add(new Object[]{"LN", 1});
        result.add(new Object[]{"LOCAL", 1});
        result.add(new Object[]{"LOCALTIME", 1});
        result.add(new Object[]{"LOCALTIMESTAMP", 1});
        result.add(new Object[]{"LOWER", 1});
        result.add(new Object[]{"MATCH", 1});
        result.add(new Object[]{"MATCHES", 1});
        result.add(new Object[]{"MATCH_NUMBER", 1});
        result.add(new Object[]{"MATCH_RECOGNIZE", 1});
        result.add(new Object[]{"MAX", 1});
        result.add(new Object[]{"MEASURES", 1});
        result.add(new Object[]{"MEMBER", 1});
        result.add(new Object[]{"MERGE", 1});
        result.add(new Object[]{"METHOD", 1});
        result.add(new Object[]{"MIN", 1});
        result.add(new Object[]{"MINUTE", 1});
        result.add(new Object[]{"MOD", 1});
        result.add(new Object[]{"MODIFIES", 1});
        result.add(new Object[]{"MODULE", 1});
        result.add(new Object[]{"MONDAY", 1});
        result.add(new Object[]{"MONTH", 1});
        result.add(new Object[]{"MORE", 1});
        result.add(new Object[]{"MULTISET", 1});
        result.add(new Object[]{"NATIONAL", 1});
        result.add(new Object[]{"NATURAL", 1});
        result.add(new Object[]{"NCHAR", 1});
        result.add(new Object[]{"NCLOB", 1});
        result.add(new Object[]{"NEW", 1});
        result.add(new Object[]{"NEXT", 1});
        result.add(new Object[]{"NO", 1});
        result.add(new Object[]{"NONE", 1});
        result.add(new Object[]{"NORMALIZE", 1});
        result.add(new Object[]{"NOT", 1});
        result.add(new Object[]{"NTH_VALUE", 1});
        result.add(new Object[]{"NTILE", 1});
        result.add(new Object[]{"NULL", 1});
        result.add(new Object[]{"NULLIF", 1});
        result.add(new Object[]{"NUMERIC", 1});
        result.add(new Object[]{"OCCURRENCES_REGEX", 1});
        result.add(new Object[]{"OCTET_LENGTH", 1});
        result.add(new Object[]{"OF", 1});
        result.add(new Object[]{"OFFSET", 1});
        result.add(new Object[]{"OLD", 1});
        result.add(new Object[]{"OMIT", 1});
        result.add(new Object[]{"ON", 1});
        result.add(new Object[]{"ONE", 1});
        result.add(new Object[]{"ONLY", 1});
        result.add(new Object[]{"OPEN", 1});
        result.add(new Object[]{"OR", 1});
        result.add(new Object[]{"ORDER", 1});
        result.add(new Object[]{"OUT", 1});
        result.add(new Object[]{"OUTER", 1});
        result.add(new Object[]{"OVER", 1});
        result.add(new Object[]{"OVERLAPS", 1});
        result.add(new Object[]{"OVERLAY", 1});
        result.add(new Object[]{"PARAMETER", 1});
        result.add(new Object[]{"PARTITION", 1});
        result.add(new Object[]{"PATTERN", 1});
        result.add(new Object[]{"PER", 1});
        result.add(new Object[]{"PERCENT", 1});
        result.add(new Object[]{"PERCENTILE_CONT", 1});
        result.add(new Object[]{"PERCENTILE_DISC", 1});
        result.add(new Object[]{"PERCENT_RANK", 1});
        result.add(new Object[]{"PERIOD", 1});
        result.add(new Object[]{"PERMUTE", 1});
        result.add(new Object[]{"PORTION", 1});
        result.add(new Object[]{"POSITION", 1});
        result.add(new Object[]{"POSITION_REGEX", 1});
        result.add(new Object[]{"POWER", 1});
        result.add(new Object[]{"PRECEDES", 1});
        result.add(new Object[]{"PRECISION", 1});
        result.add(new Object[]{"PREV", 1});
        result.add(new Object[]{"PRIMARY", 1});
        result.add(new Object[]{"PROCEDURE", 1});
        result.add(new Object[]{"PURGE_RESOURCES", 1});
        result.add(new Object[]{"RANGE", 1});
        result.add(new Object[]{"RANK", 1});
        result.add(new Object[]{"READS", 1});
        result.add(new Object[]{"REAL", 1});
        result.add(new Object[]{"RECURSIVE", 1});
        result.add(new Object[]{"REF", 1});
        result.add(new Object[]{"REFERENCES", 1});
        result.add(new Object[]{"REFERENCING", 1});
        result.add(new Object[]{"REGR_AVGX", 1});
        result.add(new Object[]{"REGR_AVGY", 1});
        result.add(new Object[]{"REGR_COUNT", 1});
        result.add(new Object[]{"REGR_INTERCEPT", 1});
        result.add(new Object[]{"REGR_R2", 1});
        result.add(new Object[]{"REGR_SLOPE", 1});
        result.add(new Object[]{"REGR_SXX", 1});
        result.add(new Object[]{"REGR_SXY", 1});
        result.add(new Object[]{"REGR_SYY", 1});
        result.add(new Object[]{"RELEASE", 1});
        result.add(new Object[]{"RESET", 1});
        result.add(new Object[]{"RESULT", 1});
        result.add(new Object[]{"RETURN", 1});
        result.add(new Object[]{"RETURNS", 1});
        result.add(new Object[]{"REVOKE", 1});
        result.add(new Object[]{"RIGHT", 1});
        result.add(new Object[]{"ROLLBACK", 1});
        result.add(new Object[]{"ROLLUP", 1});
        result.add(new Object[]{"ROW", 1});
        result.add(new Object[]{"ROW_NUMBER", 1});
        result.add(new Object[]{"ROWS", 1});
        result.add(new Object[]{"RUNNING", 1});
        result.add(new Object[]{"SATURDAY", 1});
        result.add(new Object[]{"SAVEPOINT", 1});
        result.add(new Object[]{"SCOPE", 1});
        result.add(new Object[]{"SCROLL", 1});
        result.add(new Object[]{"SEARCH", 1});
        result.add(new Object[]{"SECOND", 1});
        result.add(new Object[]{"SEEK", 1});
        result.add(new Object[]{"SELECT", 1});
        result.add(new Object[]{"SENSITIVE", 1});
        result.add(new Object[]{"SESSION_USER", 1});
        result.add(new Object[]{"SET", 1});
        result.add(new Object[]{"MINUS", 1});
        result.add(new Object[]{"SHOW", 1});
        result.add(new Object[]{"SIMILAR", 1});
        result.add(new Object[]{"SKIP", 1});
        result.add(new Object[]{"SMALLINT", 1});
        result.add(new Object[]{"SOME", 1});
        result.add(new Object[]{"SPECIFIC", 1});
        result.add(new Object[]{"SPECIFICTYPE", 1});
        result.add(new Object[]{"SQL", 1});
        result.add(new Object[]{"SQLEXCEPTION", 1});
        result.add(new Object[]{"SQLSTATE", 1});
        result.add(new Object[]{"SQLWARNING", 1});
        result.add(new Object[]{"SQRT", 1});
        result.add(new Object[]{"START", 1});
        result.add(new Object[]{"STATIC", 1});
        result.add(new Object[]{"STDDEV_POP", 1});
        result.add(new Object[]{"STDDEV_SAMP", 1});
        result.add(new Object[]{"STREAM", 1});
        result.add(new Object[]{"SUBMULTISET", 1});
        result.add(new Object[]{"SUBSET", 1});
        result.add(new Object[]{"SUBSTR", 1});
        result.add(new Object[]{"SUBSTRING", 1});
        result.add(new Object[]{"SUBSTRING_REGEX", 1});
        result.add(new Object[]{"SUBSTRING_INDEX", 1});
        result.add(new Object[]{"SUCCEEDS", 1});
        result.add(new Object[]{"SUM", 1});
        result.add(new Object[]{"SUNDAY", 1});
        result.add(new Object[]{"SYMMETRIC", 1});
        result.add(new Object[]{"SYSTEM", 1});
        result.add(new Object[]{"SYSTEM_TIME", 1});
        result.add(new Object[]{"SYSTEM_USER", 1});
        result.add(new Object[]{"TABLE", 1});
        result.add(new Object[]{"TABLESAMPLE", 1});
        result.add(new Object[]{"THEN", 1});
        result.add(new Object[]{"THURSDAY", 1});
        result.add(new Object[]{"TIME", 1});
        result.add(new Object[]{"TIMESTAMP", 1});
        result.add(new Object[]{"TIMEZONE_HOUR", 1});
        result.add(new Object[]{"TIMEZONE_MINUTE", 1});
        result.add(new Object[]{"TINYINT", 1});
        result.add(new Object[]{"TO", 1});
        result.add(new Object[]{"TRAILING", 1});
        result.add(new Object[]{"TRANSLATE", 1});
        result.add(new Object[]{"TRANSLATE_REGEX", 1});
        result.add(new Object[]{"TRANSLATION", 1});
        result.add(new Object[]{"TREAT", 1});
        result.add(new Object[]{"TRIGGER", 1});
        result.add(new Object[]{"TRIM", 1});
        result.add(new Object[]{"TRIM_ARRAY", 1});
        result.add(new Object[]{"TRUE", 1});
        result.add(new Object[]{"TRUNCATE", 1});
        result.add(new Object[]{"TUESDAY", 1});
        result.add(new Object[]{"UESCAPE", 1});
        result.add(new Object[]{"UNION", 1});
        result.add(new Object[]{"UNIQUE", 1});
        result.add(new Object[]{"UNKNOWN", 1});
        result.add(new Object[]{"UNNEST", 1});
        result.add(new Object[]{"UPDATE", 1});
        result.add(new Object[]{"UPPER", 1});
        result.add(new Object[]{"UPSERT", 1});
        result.add(new Object[]{"USING", 1});
        result.add(new Object[]{"VALUES", 1});
        result.add(new Object[]{"VALUE_OF", 1});
        result.add(new Object[]{"VAR_POP", 1});
        result.add(new Object[]{"VAR_SAMP", 1});
        result.add(new Object[]{"VARBINARY", 1});
        result.add(new Object[]{"VARCHAR", 1});
        result.add(new Object[]{"VARYING", 1});
        result.add(new Object[]{"VERSIONING", 1});
        result.add(new Object[]{"WEDNESDAY", 1});
        result.add(new Object[]{"WHEN", 1});
        result.add(new Object[]{"WHENEVER", 1});
        result.add(new Object[]{"WHERE", 1});
        result.add(new Object[]{"WIDTH_BUCKET", 1});
        result.add(new Object[]{"WINDOW", 1});
        result.add(new Object[]{"WITH", 1});
        result.add(new Object[]{"WITHIN", 1});
        result.add(new Object[]{"WITHOUT", 1});
        result.add(new Object[]{"YEAR", 1});
        result.add(new Object[]{"ALGORITHM", 1});
        result.add(new Object[]{"AUTO_RANDOM", 1});
        result.add(new Object[]{"BATCH", 1});
        result.add(new Object[]{"CANCEL", 1});
        result.add(new Object[]{"CONCURRENT", 1});
        result.add(new Object[]{"INPLACE", 1});
        result.add(new Object[]{"IF", 1});
        result.add(new Object[]{"COPY", 1});
        result.add(new Object[]{"CODEC_VERSION", 1});
        result.add(new Object[]{"MATERIALIZED", 1});
        result.add(new Object[]{"STORED", 1});
        result.add(new Object[]{"VIRTUAL", 1});
        result.add(new Object[]{"JAR", 1});
        result.add(new Object[]{"FILE", 1});
        result.add(new Object[]{"FLASHBACK", 1});
        result.add(new Object[]{"ARCHIVE", 1});
        result.add(new Object[]{"CLUSTERED", 1});
        result.add(new Object[]{"NONCLUSTERED", 1});
        result.add(new Object[]{"DATABASES", 1});
        result.add(new Object[]{"DINGOENGINES", 1});
        result.add(new Object[]{"DISTRIBUTION", 1});
        result.add(new Object[]{"ENCLOSED", 1});
        result.add(new Object[]{"EXCHANGE", 1});
        result.add(new Object[]{"EXCLUSIVE", 1});
        result.add(new Object[]{"EXECUTOR", 1});
        result.add(new Object[]{"EXECUTORS", 1});
        result.add(new Object[]{"ENFORCED", 1});
        result.add(new Object[]{"ENCRYPTION", 1});
        result.add(new Object[]{"ESCAPED", 1});
        result.add(new Object[]{"FIELDS", 1});
        result.add(new Object[]{"FLUSH", 1});
        result.add(new Object[]{"GRANTS", 1});
        result.add(new Object[]{"IDENTIFIED", 1});
        result.add(new Object[]{"INDEX", 1});
        result.add(new Object[]{"INFILE", 1});
        result.add(new Object[]{"INVISIBLE", 1});
        result.add(new Object[]{"LESS", 1});
        result.add(new Object[]{"LINES", 1});
        result.add(new Object[]{"LOAD", 1});
        result.add(new Object[]{"LOCK", 1});
        result.add(new Object[]{"NEXT_AUTO_INCREMENT", 1});
        result.add(new Object[]{"PARAMETERS", 1});
        result.add(new Object[]{"PLUGINS", 1});
        result.add(new Object[]{"PREPARE", 1});
        result.add(new Object[]{"PROCESSLIST", 1});
        result.add(new Object[]{"RECOVER", 1});
        result.add(new Object[]{"RELOAD", 1});
        result.add(new Object[]{"REQUIRE", 1});
        result.add(new Object[]{"SCAN", 1});
        result.add(new Object[]{"SSL", 1});
        result.add(new Object[]{"SERIAL", 1});
        result.add(new Object[]{"STARTING", 1});
        result.add(new Object[]{"SHARED", 1});
        result.add(new Object[]{"STORAGE", 1});
        result.add(new Object[]{"ROW_FORMAT", 1});
        result.add(new Object[]{"STARTTS", 1});
        result.add(new Object[]{"TERMINATED", 1});
        result.add(new Object[]{"THAN", 1});
        result.add(new Object[]{"TTL", 1});
        result.add(new Object[]{"TRACE", 1});
        result.add(new Object[]{"TSO", 1});
        result.add(new Object[]{"UNLOCK", 1});
        result.add(new Object[]{"USE", 1});
        result.add(new Object[]{"VARIABLES", 1});
        result.add(new Object[]{"VECTOR", 1});
        result.add(new Object[]{"VISIBLE", 1});
        result.add(new Object[]{"WARNINGS", 1});
        result.add(new Object[]{"ANALYZE", 1});
        result.add(new Object[]{"BUCKETS", 1});
        result.add(new Object[]{"CMSKETCH", 1});
        result.add(new Object[]{"SAMPLES", 1});
        result.add(new Object[]{"SAMPLERATE", 1});
        result.add(new Object[]{"PESSIMISTIC", 1});
        result.add(new Object[]{"PARSER", 1});
        result.add(new Object[]{"OPTIMISTIC", 1});
        result.add(new Object[]{"BLOCKS", 1});
        result.add(new Object[]{"HASH", 1});
        result.add(new Object[]{"LOCKS", 1});
        result.add(new Object[]{"COMMENT", 1});
        result.add(new Object[]{"CHARSET", 1});
        result.add(new Object[]{"KILL", 1});
        result.add(new Object[]{"QUERY", 1});
        result.add(new Object[]{"UNSIGNED", 1});
        result.add(new Object[]{"TEXT", 1});
        result.add(new Object[]{"LONGTEXT", 1});
        result.add(new Object[]{"OUTFILE", 1});
        result.add(new Object[]{"MONITOR", 1});
        result.add(new Object[]{"REGIONS", 1});
        result.add(new Object[]{"TENANT", 1});
        result.add(new Object[]{"TENANTS", 1});
        result.add(new Object[]{"RENAME", 1});
        result.add(new Object[]{"TEXT_SEARCH", 1});
        result.add(new Object[]{"HYBRID_SEARCH", 1});
        result.add(new Object[]{"DISK", 1});
        result.add(new Object[]{"DISK_ANN_BUILD", 1});
        result.add(new Object[]{"DISK_ANN_LOAD", 1});
        result.add(new Object[]{"DISK_ANN_STATUS", 1});
        result.add(new Object[]{"DISK_ANN_RESET", 1});
        result.add(new Object[]{"DISK_ANN_COUNT_MEMORY", 1});
        result.add(new Object[]{"BACK_UP_TIME_POINT", 1});
        result.add(new Object[]{"BACK_UP_TSO_POINT", 1});
        result.add(new Object[]{"START_GC", 1});
        result.add(new Object[]{"CACHE", 1});
        result.add(new Object[]{"MODIFY", 1});
        result.add(new Object[]{"CHANGE", 1});
        result.add(new Object[]{"SPATIAL", 1});
        result.add(new Object[]{"FULLTEXT", 1});
        result.add(new Object[]{"KEY_BLOCK_SIZE", 1});
        result.add(new Object[]{"BTREE", 1});
        result.add(new Object[]{"RTREE", 1});
        result.add(new Object[]{"UNDEFINED", 1});
        result.add(new Object[]{"TEMPTABLE", 1});
        result.add(new Object[]{"FIXED", 1});
        result.add(new Object[]{"MEMORY", 1});
        result.add(new Object[]{"COLUMN_FORMAT", 1});
        result.add(new Object[]{"A", 0});
        result.add(new Object[]{"ABSENT", 0});
        result.add(new Object[]{"ABSOLUTE", 0});
        result.add(new Object[]{"ACCOUNT", 0});
        result.add(new Object[]{"AUTO_INCREMENT", 0});
        result.add(new Object[]{"ACTION", 0});
        result.add(new Object[]{"ADA", 0});
        result.add(new Object[]{"ADD", 0});
        result.add(new Object[]{"ADMIN", 0});
        result.add(new Object[]{"AFTER", 0});
        result.add(new Object[]{"ALWAYS", 0});
        result.add(new Object[]{"APPLY", 0});
        result.add(new Object[]{"ARRAY_AGG", 0});
        result.add(new Object[]{"ARRAY_CONCAT_AGG", 0});
        result.add(new Object[]{"ASC", 0});
        result.add(new Object[]{"ASSERTION", 0});
        result.add(new Object[]{"ASSIGNMENT", 0});
        result.add(new Object[]{"ATTRIBUTE", 0});
        result.add(new Object[]{"ATTRIBUTES", 0});
        result.add(new Object[]{"BLOB", 0});
        result.add(new Object[]{"BEFORE", 0});
        result.add(new Object[]{"BERNOULLI", 0});
        result.add(new Object[]{"BREADTH", 0});
        result.add(new Object[]{"C", 0});
        result.add(new Object[]{"CASCADE", 0});
        result.add(new Object[]{"CATALOG", 0});
        result.add(new Object[]{"CATALOG_NAME", 0});
        result.add(new Object[]{"CENTURY", 0});
        result.add(new Object[]{"CHAIN", 0});
        result.add(new Object[]{"CHARACTERISTICS", 0});
        result.add(new Object[]{"CHARACTERS", 0});
        result.add(new Object[]{"CHARACTER_SET_CATALOG", 0});
        result.add(new Object[]{"CHARACTER_SET_NAME", 0});
        result.add(new Object[]{"CHARACTER_SET_SCHEMA", 0});
        result.add(new Object[]{"CLASS_ORIGIN", 0});
        result.add(new Object[]{"COBOL", 0});
        result.add(new Object[]{"COLLATION", 0});
        result.add(new Object[]{"COLLATION_CATALOG", 0});
        result.add(new Object[]{"COLLATION_NAME", 0});
        result.add(new Object[]{"COLLATION_SCHEMA", 0});
        result.add(new Object[]{"COLUMNS", 0});
        result.add(new Object[]{"COLUMN_NAME", 0});
        result.add(new Object[]{"COMMAND_FUNCTION", 0});
        result.add(new Object[]{"COMMAND_FUNCTION_CODE", 0});
        result.add(new Object[]{"COMMITTED", 0});
        result.add(new Object[]{"CONDITIONAL", 0});
        result.add(new Object[]{"CONDITION_NUMBER", 0});
        result.add(new Object[]{"CONNECTION", 0});
        result.add(new Object[]{"CONNECTION_NAME", 0});
        result.add(new Object[]{"CONSTRAINT_CATALOG", 0});
        result.add(new Object[]{"CONSTRAINT_NAME", 0});
        result.add(new Object[]{"CONSTRAINTS", 0});
        result.add(new Object[]{"CONSTRAINT_SCHEMA", 0});
        result.add(new Object[]{"CONSTRUCTOR", 0});
        result.add(new Object[]{"CONTINUE", 0});
        result.add(new Object[]{"CURSOR_NAME", 0});
        result.add(new Object[]{"DATA", 0});
        result.add(new Object[]{"DATABASE", 0});
        result.add(new Object[]{"DATE", 0});
        result.add(new Object[]{"DATE_TRUNC", 0});
        result.add(new Object[]{"DATETIME_INTERVAL_CODE", 0});
        result.add(new Object[]{"DATETIME_INTERVAL_PRECISION", 0});
        result.add(new Object[]{"DAYS", 0});
        result.add(new Object[]{"DECADE", 0});
        result.add(new Object[]{"DEFAULTS", 0});
        result.add(new Object[]{"DEFERRABLE", 0});
        result.add(new Object[]{"DEFERRED", 0});
        result.add(new Object[]{"DEFINED", 0});
        result.add(new Object[]{"DEFINER", 0});
        result.add(new Object[]{"DEGREE", 0});
        result.add(new Object[]{"DEPTH", 0});
        result.add(new Object[]{"DERIVED", 0});
        result.add(new Object[]{"DESC", 0});
        result.add(new Object[]{"DESCRIPTION", 0});
        result.add(new Object[]{"DESCRIPTOR", 0});
        result.add(new Object[]{"DIAGNOSTICS", 0});
        result.add(new Object[]{"DISPATCH", 0});
        result.add(new Object[]{"DOMAIN", 0});
        result.add(new Object[]{"DOW", 0});
        result.add(new Object[]{"DOY", 0});
        result.add(new Object[]{"DOT_FORMAT", 0});
        result.add(new Object[]{"DYNAMIC_FUNCTION", 0});
        result.add(new Object[]{"DYNAMIC_FUNCTION_CODE", 0});
        result.add(new Object[]{"ENCODING", 0});
        result.add(new Object[]{"EPOCH", 0});
        result.add(new Object[]{"ERROR", 0});
        result.add(new Object[]{"ENGINE", 0});
        result.add(new Object[]{"EXCEPTION", 0});
        result.add(new Object[]{"EXCLUDE", 0});
        result.add(new Object[]{"EXCLUDING", 0});
        result.add(new Object[]{"EXPIRE", 0});
        result.add(new Object[]{"FINAL", 0});
        result.add(new Object[]{"FIRST", 0});
        result.add(new Object[]{"FOLLOWING", 0});
        result.add(new Object[]{"FORMAT", 0});
        result.add(new Object[]{"FORTRAN", 0});
        result.add(new Object[]{"FOUND", 0});
        result.add(new Object[]{"FRAC_SECOND", 0});
        result.add(new Object[]{"G", 0});
        result.add(new Object[]{"GENERAL", 0});
        result.add(new Object[]{"GENERATED", 0});
        result.add(new Object[]{"GEOMETRY", 0});
        result.add(new Object[]{"GLOBAL", 0});
        result.add(new Object[]{"GO", 0});
        result.add(new Object[]{"GOTO", 0});
        result.add(new Object[]{"GRANTED", 0});
        result.add(new Object[]{"GROUP_CONCAT", 0});
        result.add(new Object[]{"HIERARCHY", 0});
        result.add(new Object[]{"HOP", 0});
        result.add(new Object[]{"HOURS", 0});
        result.add(new Object[]{"IGNORE", 0});
        result.add(new Object[]{"ILIKE", 0});
        result.add(new Object[]{"IMMEDIATE", 0});
        result.add(new Object[]{"IMMEDIATELY", 0});
        result.add(new Object[]{"IMPLEMENTATION", 0});
        result.add(new Object[]{"INCLUDE", 0});
        result.add(new Object[]{"INCLUDING", 0});
        result.add(new Object[]{"INCREMENT", 0});
        result.add(new Object[]{"INITIALLY", 0});
        result.add(new Object[]{"INPUT", 0});
        result.add(new Object[]{"INSTANCE", 0});
        result.add(new Object[]{"INSTANTIABLE", 0});
        result.add(new Object[]{"INVOKER", 0});
        result.add(new Object[]{"ISODOW", 0});
        result.add(new Object[]{"ISOLATION", 0});
        result.add(new Object[]{"ISOYEAR", 0});
        result.add(new Object[]{"JAVA", 0});
        result.add(new Object[]{"JSON", 0});
        result.add(new Object[]{"K", 0});
        result.add(new Object[]{"KEY_MEMBER", 0});
        result.add(new Object[]{"KEY_TYPE", 0});
        result.add(new Object[]{"LABEL", 0});
        result.add(new Object[]{"LAST", 0});
        result.add(new Object[]{"LENGTH", 0});
        result.add(new Object[]{"LEVEL", 0});
        result.add(new Object[]{"LIBRARY", 0});
        result.add(new Object[]{"LOCATOR", 0});
        result.add(new Object[]{"M", 0});
        result.add(new Object[]{"MAP", 0});
        result.add(new Object[]{"MATCHED", 0});
        result.add(new Object[]{"MAXVALUE", 0});
        result.add(new Object[]{"MESSAGE_LENGTH", 0});
        result.add(new Object[]{"MESSAGE_OCTET_LENGTH", 0});
        result.add(new Object[]{"MESSAGE_TEXT", 0});
        result.add(new Object[]{"MICROSECOND", 0});
        result.add(new Object[]{"MILLENNIUM", 0});
        result.add(new Object[]{"MILLISECOND", 0});
        result.add(new Object[]{"MINUTES", 0});
        result.add(new Object[]{"MINVALUE", 0});
        result.add(new Object[]{"MONTHS", 0});
        result.add(new Object[]{"MORE_", 0});
        result.add(new Object[]{"MUMPS", 0});
        result.add(new Object[]{"NAME", 0});
        result.add(new Object[]{"NAMES", 0});
        result.add(new Object[]{"NANOSECOND", 0});
        result.add(new Object[]{"NESTING", 0});
        result.add(new Object[]{"NORMALIZED", 0});
        result.add(new Object[]{"NULLABLE", 0});
        result.add(new Object[]{"NULLS", 0});
        result.add(new Object[]{"NUMBER", 0});
        result.add(new Object[]{"OBJECT", 0});
        result.add(new Object[]{"OCTETS", 0});
        result.add(new Object[]{"OPTION", 0});
        result.add(new Object[]{"OPTIONS", 0});
        result.add(new Object[]{"ORDERING", 0});
        result.add(new Object[]{"ORDINALITY", 0});
        result.add(new Object[]{"OTHERS", 0});
        result.add(new Object[]{"OUTPUT", 0});
        result.add(new Object[]{"OVERRIDING", 0});
        result.add(new Object[]{"PAD", 0});
        result.add(new Object[]{"PARAMETER_MODE", 0});
        result.add(new Object[]{"PARAMETER_NAME", 0});
        result.add(new Object[]{"PARAMETER_ORDINAL_POSITION", 0});
        result.add(new Object[]{"PARAMETER_SPECIFIC_CATALOG", 0});
        result.add(new Object[]{"PARAMETER_SPECIFIC_NAME", 0});
        result.add(new Object[]{"PARAMETER_SPECIFIC_SCHEMA", 0});
        result.add(new Object[]{"PARTIAL", 0});
        result.add(new Object[]{"PARTITIONS", 0});
        result.add(new Object[]{"PASCAL", 0});
        result.add(new Object[]{"PASSING", 0});
        result.add(new Object[]{"PASSTHROUGH", 0});
        result.add(new Object[]{"PASSWORD", 0});
        result.add(new Object[]{"PAST", 0});
        result.add(new Object[]{"PATH", 0});
        result.add(new Object[]{"PIVOT", 0});
        result.add(new Object[]{"PLACING", 0});
        result.add(new Object[]{"PLAN", 0});
        result.add(new Object[]{"PLI", 0});
        result.add(new Object[]{"PRECEDING", 0});
        result.add(new Object[]{"PRESERVE", 0});
        result.add(new Object[]{"PRIOR", 0});
        result.add(new Object[]{"PRIVILEGES", 0});
        result.add(new Object[]{"PUBLIC", 0});
        result.add(new Object[]{"QUARTER", 0});
        result.add(new Object[]{"QUARTERS", 0});
        result.add(new Object[]{"READ", 0});
        result.add(new Object[]{"RELATIVE", 0});
        result.add(new Object[]{"REMARKS", 0});
        result.add(new Object[]{"REPEATABLE", 0});
        result.add(new Object[]{"REPLACE", 0});
        result.add(new Object[]{"REPLICA", 0});
        result.add(new Object[]{"RESPECT", 0});
        result.add(new Object[]{"RESTART", 0});
        result.add(new Object[]{"RESTRICT", 0});
        result.add(new Object[]{"RETURNED_CARDINALITY", 0});
        result.add(new Object[]{"RETURNED_LENGTH", 0});
        result.add(new Object[]{"RETURNED_OCTET_LENGTH", 0});
        result.add(new Object[]{"RETURNED_SQLSTATE", 0});
        result.add(new Object[]{"RETURNING", 0});
        result.add(new Object[]{"RLIKE", 0});
        result.add(new Object[]{"ROLE", 0});
        result.add(new Object[]{"ROUTINE", 0});
        result.add(new Object[]{"ROUTINE_CATALOG", 0});
        result.add(new Object[]{"ROUTINE_NAME", 0});
        result.add(new Object[]{"ROUTINE_SCHEMA", 0});
        result.add(new Object[]{"ROW_COUNT", 0});
        result.add(new Object[]{"SCALAR", 0});
        result.add(new Object[]{"SCALE", 0});
        result.add(new Object[]{"SCHEMA", 0});
        result.add(new Object[]{"SCHEMA_NAME", 0});
        result.add(new Object[]{"SCOPE_CATALOGS", 0});
        result.add(new Object[]{"SCOPE_NAME", 0});
        result.add(new Object[]{"SCOPE_SCHEMA", 0});
        result.add(new Object[]{"SECONDS", 0});
        result.add(new Object[]{"SECTION", 0});
        result.add(new Object[]{"SECURITY", 0});
        result.add(new Object[]{"SELF", 0});
        result.add(new Object[]{"SEPARATOR", 0});
        result.add(new Object[]{"SEQUENCE", 0});
        result.add(new Object[]{"SERIALIZABLE", 0});
        result.add(new Object[]{"SERVER", 0});
        result.add(new Object[]{"SERVER_NAME", 0});
        result.add(new Object[]{"SESSION", 0});
        result.add(new Object[]{"SETS", 0});
        result.add(new Object[]{"SIMPLE", 0});
        result.add(new Object[]{"SIZE", 0});
        result.add(new Object[]{"SOURCE", 0});
        result.add(new Object[]{"SPACE", 0});
        result.add(new Object[]{"SPECIFIC_NAME", 0});
        result.add(new Object[]{"SQL_BIGINT", 0});
        result.add(new Object[]{"SQL_BINARY", 0});
        result.add(new Object[]{"SQL_BIT", 0});
        result.add(new Object[]{"SQL_BLOB", 0});
        result.add(new Object[]{"SQL_BOOLEAN", 0});
        result.add(new Object[]{"SQL_CHAR", 0});
        result.add(new Object[]{"SQL_CLOB", 0});
        result.add(new Object[]{"SQL_DATE", 0});
        result.add(new Object[]{"SQL_DECIMAL", 0});
        result.add(new Object[]{"SQL_DOUBLE", 0});
        result.add(new Object[]{"SQL_FLOAT", 0});
        result.add(new Object[]{"SQL_INTEGER", 0});
        result.add(new Object[]{"SQL_INTERVAL_DAY", 0});
        result.add(new Object[]{"SQL_INTERVAL_DAY_TO_HOUR", 0});
        result.add(new Object[]{"SQL_INTERVAL_DAY_TO_MINUTE", 0});
        result.add(new Object[]{"SQL_INTERVAL_DAY_TO_SECOND", 0});
        result.add(new Object[]{"SQL_INTERVAL_HOUR", 0});
        result.add(new Object[]{"SQL_INTERVAL_HOUR_TO_MINUTE", 0});
        result.add(new Object[]{"SQL_INTERVAL_HOUR_TO_SECOND", 0});
        result.add(new Object[]{"SQL_INTERVAL_MINUTE", 0});
        result.add(new Object[]{"SQL_INTERVAL_MINUTE_TO_SECOND", 0});
        result.add(new Object[]{"SQL_INTERVAL_MONTH", 0});
        result.add(new Object[]{"SQL_INTERVAL_SECOND", 0});
        result.add(new Object[]{"SQL_INTERVAL_YEAR", 0});
        result.add(new Object[]{"SQL_INTERVAL_YEAR_TO_MONTH", 0});
        result.add(new Object[]{"SQL_LONGVARBINARY", 0});
        result.add(new Object[]{"SQL_LONGVARCHAR", 0});
        result.add(new Object[]{"SQL_LONGVARNCHAR", 0});
        result.add(new Object[]{"SQL_NCHAR", 0});
        result.add(new Object[]{"SQL_NCLOB", 0});
        result.add(new Object[]{"SQL_NUMERIC", 0});
        result.add(new Object[]{"SQL_NVARCHAR", 0});
        result.add(new Object[]{"SQL_REAL", 0});
        result.add(new Object[]{"SQL_SMALLINT", 0});
        result.add(new Object[]{"SQL_TIME", 0});
        result.add(new Object[]{"SQL_TIMESTAMP", 0});
        result.add(new Object[]{"SQL_TINYINT", 0});
        result.add(new Object[]{"SQL_TSI_DAY", 0});
        result.add(new Object[]{"SQL_TSI_FRAC_SECOND", 0});
        result.add(new Object[]{"SQL_TSI_HOUR", 0});
        result.add(new Object[]{"SQL_TSI_MICROSECOND", 0});
        result.add(new Object[]{"SQL_TSI_MINUTE", 0});
        result.add(new Object[]{"SQL_TSI_MONTH", 0});
        result.add(new Object[]{"SQL_TSI_QUARTER", 0});
        result.add(new Object[]{"SQL_TSI_SECOND", 0});
        result.add(new Object[]{"SQL_TSI_WEEK", 0});
        result.add(new Object[]{"SQL_TSI_YEAR", 0});
        result.add(new Object[]{"SQL_VARBINARY", 0});
        result.add(new Object[]{"SQL_VARCHAR", 0});
        result.add(new Object[]{"STATE", 0});
        result.add(new Object[]{"STATUS", 0});
        result.add(new Object[]{"STATEMENT", 0});
        result.add(new Object[]{"STRING_AGG", 0});
        result.add(new Object[]{"STRUCTURE", 0});
        result.add(new Object[]{"STYLE", 0});
        result.add(new Object[]{"SUBCLASS_ORIGIN", 0});
        result.add(new Object[]{"SUBSTITUTE", 0});
        result.add(new Object[]{"TABLES", 0});
        result.add(new Object[]{"TABLE_NAME", 0});
        result.add(new Object[]{"TEMPORARY", 0});
        result.add(new Object[]{"TIES", 0});
        result.add(new Object[]{"TIME_DIFF", 0});
        result.add(new Object[]{"TIME_TRUNC", 0});
        result.add(new Object[]{"TIMESTAMPADD", 0});
        result.add(new Object[]{"TIMESTAMPDIFF", 0});
        result.add(new Object[]{"TIMESTAMP_DIFF", 0});
        result.add(new Object[]{"TIMESTAMP_TRUNC", 0});
        result.add(new Object[]{"TOP_LEVEL_COUNT", 0});
        result.add(new Object[]{"TRANSACTION", 0});
        result.add(new Object[]{"TRANSACTIONS_ACTIVE", 0});
        result.add(new Object[]{"TRANSACTIONS_COMMITTED", 0});
        result.add(new Object[]{"TRANSACTIONS_ROLLED_BACK", 0});
        result.add(new Object[]{"TRANSFORM", 0});
        result.add(new Object[]{"TRANSFORMS", 0});
        result.add(new Object[]{"TRIGGERS", 0});
        result.add(new Object[]{"TRIGGER_CATALOG", 0});
        result.add(new Object[]{"TRIGGER_NAME", 0});
        result.add(new Object[]{"TRIGGER_SCHEMA", 0});
        result.add(new Object[]{"TUMBLE", 0});
        result.add(new Object[]{"TYPE", 0});
        result.add(new Object[]{"UNBOUNDED", 0});
        result.add(new Object[]{"UNCOMMITTED", 0});
        result.add(new Object[]{"UNCONDITIONAL", 0});
        result.add(new Object[]{"UNDER", 0});
        result.add(new Object[]{"UNPIVOT", 0});
        result.add(new Object[]{"UNNAMED", 0});
        result.add(new Object[]{"USAGE", 0});
        result.add(new Object[]{"USER", 0});
        result.add(new Object[]{"USER_DEFINED_TYPE_CATALOG", 0});
        result.add(new Object[]{"USER_DEFINED_TYPE_CODE", 0});
        result.add(new Object[]{"USER_DEFINED_TYPE_NAME", 0});
        result.add(new Object[]{"USER_DEFINED_TYPE_SCHEMA", 0});
        result.add(new Object[]{"UTF16", 0});
        result.add(new Object[]{"UTF32", 0});
        result.add(new Object[]{"UTF8", 0});
        result.add(new Object[]{"VERSION", 0});
        result.add(new Object[]{"VIEW", 0});
        result.add(new Object[]{"WEEK", 0});
        result.add(new Object[]{"WEEKS", 0});
        result.add(new Object[]{"WORK", 0});
        result.add(new Object[]{"WRAPPER", 0});
        result.add(new Object[]{"WRITE", 0});
        result.add(new Object[]{"XML", 0});
        result.add(new Object[]{"YEARS", 0});
        result.add(new Object[]{"ZONE", 0});
        result.add(new Object[]{"VALUE", 0});

        return result.stream().iterator();
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
