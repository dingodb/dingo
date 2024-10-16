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

import io.dingodb.common.profile.StmtSummaryMap;
import io.dingodb.common.log.LogUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.InfoSchemaScanParam;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Partition;
import io.dingodb.meta.entity.Table;
import io.dingodb.transaction.api.TransactionService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.util.ArrayList;
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
            case "EVENTS":
            case "TRIGGERS":
            case "ROUTINES":
            case "FILES":
            case "KEY_COLUMN_USAGE":
            case "COLUMN_STATISTICS":
            case "USER_PRIVILEGES":
            case "SCHEMA_PRIVILEGES":
            case "TABLE_PRIVILEGES":
            case "VIEWS":
            case "COLUMN_PRIVILEGES":
            case "COLLATIONS":
                return getEmpty();
            case "TABLE_CONSTRAINTS":
                return getInformationTableConstraints();
            case "STATEMENTS_SUMMARY":
                return StmtSummaryMap.iterator();
            case "DINGO_MDL_VIEW":
                return getMdlView();
            case "DINGO_TRX":
                return getTxnInfo();
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
                .flatMap(table -> {
                    if (table.partitions == null || table.getPartitions().isEmpty()) {
                        LogUtils.warn(log, "The table {} not have partition, please check meta.", table.name);
                        return Stream.<Object[]>of(getPartitionDetail(schemaTables.getSchemaInfo().getName(), table, null));
                    } else {
                        return table.getPartitions()
                            .stream()
                            .map(partition -> getPartitionDetail(schemaTables.getSchemaInfo().getName(), table, partition));
                    }
                }))
            .iterator();
    }

    private static Object[] getPartitionDetail(String schemaName, Table td, Partition partition) {
        return new Object[]{
            "def",
            schemaName,
            td.getName(),
            // part name
            null,
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
            partition.operand,
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

    private static Iterator<Object[]> getTxnInfo() {
        return TransactionService.getDefault().getTxnInfo();
    }

}
