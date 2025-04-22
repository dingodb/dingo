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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            case "EVENTS":
            case "TRIGGERS":
            case "ROUTINES":
            case "FILES":
            case "KEY_COLUMN_USAGE":
            case "COLUMN_STATISTICS":
            case "SCHEMA_PRIVILEGES":
            case "TABLE_PRIVILEGES":
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
                return gxEngineInfos();
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
                        String type = column.getSqlTypeName().toLowerCase();
                        if (type.equalsIgnoreCase("integer")) {
                            type = "int";
                        } else if (type.equalsIgnoreCase("timestamp")) {
                            type = "datetime";
                        }
                        String columnType = type;
                        if (column.getPrecision() > 0 && column.getScale() > 0) {
                            columnType = columnType + "(" + column.getPrecision() + "," + column.getScale() + ")";
                        } else if (column.getPrecision() > 0) {
                            columnType = columnType + "(" + column.getPrecision() + ")";
                        }
                        String characterSet = td.charset;
                        String collation = "utf8_bin";
                        if (!type.equalsIgnoreCase("varchar")) {
                            characterSet = null;
                            collation = null;
                        }
                        String extra = "";
                        if (column.isAutoIncrement()) {
                            extra = "auto_increment";
                        }
                        Integer numberPrecision = null;
                        Integer numberScale = null;
                        if (!type.equalsIgnoreCase("varchar")
                            && !type.equalsIgnoreCase("datetime") && !type.equalsIgnoreCase("char")) {
                            if (type.equalsIgnoreCase("bigint") && column.getPrecision() < 0) {
                                numberPrecision = 19;
                                numberScale = 0;
                            } else if (type.equalsIgnoreCase("int") && column.getPrecision() < 0) {
                                numberPrecision = 10;
                                numberScale = 0;
                            } else if (type.equalsIgnoreCase("tinyint") && column.getPrecision() < 0) {
                                numberPrecision = 3;
                                numberScale = 0;
                            } else {
                                numberPrecision = column.getPrecision();
                                numberScale = column.getScale();
                            }
                        }

                        colRes.add(new Object[]{
                            "def",
                            schemaTables.getSchemaInfo().getName().toLowerCase(),
                            td.getName().toLowerCase(),
                            column.name.toLowerCase(),
                            // ordinal position
                            i + 1L,
                            // default value
                            column.defaultValueExpr,
                            // is null
                            column.isNullable() ? "YES" : "NO",
                            // type name
                            type,
                            (long) column.precision,
                            null,
                            numberPrecision,
                            numberScale,
                            null,
                            characterSet,
                            collation,
                            columnType,
                            // is key
                            column.isPrimary() ? "PRI" : "",
                            extra,
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
        if (partition.getOperand() != null) {
            operand = Arrays.toString(partition.getOperand());
        }
        return new Object[]{
            "def",
            schemaName.toLowerCase(),
            td.getName().toLowerCase(),
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
            .map(service -> new Object[]{"def", service.toLowerCase(), "utf8", "utf8_bin", null})
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
                                e.getSchemaInfo().getName().toLowerCase(),
                                td.getName().toLowerCase(),
                                td.tableType,
                                "InnoDB",
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
                        e.getSchemaInfo().getName().toLowerCase(),
                        "PRIMARY",
                        e.getSchemaInfo().getName().toLowerCase(),
                        td.getName().toLowerCase(),
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
                            e.getSchemaInfo().getName().toLowerCase(),
                            table.name.toLowerCase(),
                            0,
                            e.getSchemaInfo().getName().toLowerCase(),
                            "PRIMARY",
                            column.primaryKeyIndex + 1,
                            column.name.toLowerCase(),
                            "A",
                            0,
                            null,
                            null,
                            "",
                            "InnoDB",
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

    private static Iterator<Object[]> gxEngineInfos() {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{"ndbcluster", "Clustered, fault-tolerant tables", null, "NO", null, null});

        return result.stream().iterator();
    }

    private static Iterator<Object[]> getUserPrivileges() {
        List<Object[]> response = new ArrayList<>();

        response.add(new Object[]{"'root'@'%'", "def", "SELECT", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "INSERT", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "UPDATE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "DELETE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "DROP", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "PROCESS", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "REFERENCES", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "ALTER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SHOW DATABASES", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SUPER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "EXECUTE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "INDEX", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE USER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE TABLESPACE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "TRIGGER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE VIEW", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SHOW VIEW", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE ROLE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "DROP ROLE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE TEMPORARY TABLES", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "LOCK TABLES", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CREATE ROUTINE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "ALTER ROUTINE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "EVENT", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SHUTDOWN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "RELOAD", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "FILE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "REPLICATION CLIENT", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "REPLICATION SLAVE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "XA_RECOVER_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "TELEMETRY_LOG_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "TABLE_ENCRYPTION_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SYSTEM_VARIABLES_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SYSTEM_USER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SHOW_ROUTINE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SET_USER_ID", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SESSION_VARIABLES_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SERVICE_CONNECTION_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "SENSITIVE_VARIABLES_OBSERVER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "ROLE_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "RESOURCE_GROUP_USER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "RESOURCE_GROUP_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "REPLICATION_SLAVE_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "REPLICATION_APPLIER", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "PERSIST_RO_VARIABLES_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "PASSWORDLESS_USER_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "INNODB_REDO_LOG_ENABLE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "INNODB_REDO_LOG_ARCHIVE", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "GROUP_REPLICATION_STREAM", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "GROUP_REPLICATION_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "FLUSH_USER_RESOURCES", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "FLUSH_TABLES", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "FLUSH_STATUS", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "FLUSH_OPTIMIZER_COSTS", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "FIREWALL_EXEMPT", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "ENCRYPTION_KEY_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CONNECTION_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "CLONE_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "BINLOG_ENCRYPTION_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "BINLOG_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "BACKUP_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "AUTHENTICATION_POLICY_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "AUDIT_ADMIN", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "AUDIT_ABORT_EXEMPT", "YES"});
        response.add(new Object[]{"'root'@'%'", "def", "APPLICATION_PASSWORD_ADMIN", "YES"});

        return response.iterator();
    }
}
