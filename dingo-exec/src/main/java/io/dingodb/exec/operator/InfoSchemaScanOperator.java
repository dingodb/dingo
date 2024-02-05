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

import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.InfoSchemaScanParam;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Partition;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        MetaService metaService = MetaService.root();
        switch (target.toUpperCase()) {
            case "GLOBAL_VARIABLES":
                return getGlobalVariables();
            case "TABLES":
                return getInformationTables(metaService);
            case "SCHEMATA":
                return getInformationSchemata(metaService);
            case "COLUMNS":
                return getInformationColumns(metaService);
            case "PARTITIONS":
                return getInformationPartitions(metaService);
            case "EVENTS":
            case "TRIGGERS":
            case "STATISTICS":
            case "ROUTINES":
            case "KEY_COLUMN_USAGE":
                return getEmpty();
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

    private static Iterator<Object[]> getInformationColumns(MetaService metaService) {
        Map<String, MetaService> metaServiceMap = metaService.getSubMetaServices();
        return metaServiceMap
            .entrySet()
            .stream()
            .flatMap(schema -> schema.getValue().getTables()
                .stream()
                .flatMap(td -> {
                    List<Object[]> colRes = new ArrayList<>();
                    for (int i = 0; i < td.getColumns().size(); i ++) {
                        Column column = td.columns.get(i);
                        colRes.add(new Object[]{
                            "def",
                            schema.getKey(),
                            td.getName(),
                            column.name,
                            // ordinal position
                            i + 1L,
                            // default value
                            column.defaultValueExpr,
                            // is null
                            column.isNullable() ? "YES" : "NO",
                            // type name
                            column.type,
                            (long)column.precision,
                            null,
                            null,
                            null,
                            null,
                            "utf8",
                            "utf8_bin",
                            column.type.toString(),
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
                })
        ).iterator();
    }

    private static Iterator<Object[]> getInformationPartitions(MetaService metaService) {
        Map<String, MetaService> metaServiceMap = metaService.getSubMetaServices();
        return metaServiceMap.entrySet().stream().flatMap(schema -> schema.getValue().getTables()
            .stream()
            .flatMap(table -> {
                if (table.partitions == null || table.getPartitions().isEmpty()) {
                    log.warn("The table {} not have partition, please check meta.", table.name);
                    return Stream.<Object[]>of(getPartitionDetail(schema.getKey(), table, null));
                } else {
                    return table.getPartitions()
                        .stream()
                        .map(partition -> getPartitionDetail(schema.getKey(), table, partition));
                }
            })).iterator();
    }

    private static Object[] getPartitionDetail(String schemaName, Table td, Partition partition) {
        return new Object[] {
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
        Map<String, String> response = service.getGlobalVariables();
        List<Object[]> resList = response
            .entrySet()
            .stream()
            .map(e -> new Object[] {e.getKey(), e.getValue()})
            .collect(Collectors.toList());
        return resList.iterator();
    }

    private static Iterator<Object[]> getInformationSchemata(MetaService metaService) {
        Map<String, MetaService> metaServiceMap = metaService.getSubMetaServices();
        List<Object[]> schemaList = metaServiceMap
            .keySet()
            .stream()
            .map(service -> new Object[]{"def", service, "utf8", "utf8_bin", null})
            .collect(Collectors.toList());
        return schemaList.iterator();
    }

    private static Iterator<Object[]> getInformationTables(MetaService metaService) {
        Map<String, MetaService> metaServiceMap = metaService.getSubMetaServices();
        List<Object[]> tuples = metaServiceMap.entrySet()
            .stream()
            .flatMap(e -> {
                Set<Table> tables = e.getValue().getTables();
                return tables.stream()
                    .map(td -> {
                        Timestamp updateTime = null;
                        if (td.getUpdateTime() > 0) {
                            updateTime = new Timestamp(td.getUpdateTime());
                        }
                        String createOptions = "";
                        if (td.getProperties().size() > 0) {
                            createOptions = td.getProperties().toString();
                        }
                        return new Object[]{"def",
                            e.getKey(),
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
                            td.autoIncrement,
                            new Timestamp(td.getCreateTime()),
                            updateTime,
                            null,
                            td.getCollate(),
                            null,
                            createOptions,
                            td.getComment()
                        };
                    })
                    .collect(Collectors.toList()).stream();
            })
            .collect(Collectors.toList());
        return tuples.iterator();
    }
}
