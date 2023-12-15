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

import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InfoSchemaScanOperator extends FilterProjectSourceOperator {
    private String target;

    public InfoSchemaScanOperator(DingoType schema, SqlExpr filter, TupleMapping selection, String target) {
        super(schema, filter, selection);
        this.target = target;
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
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
            .flatMap(schema -> schema.getValue().getTableDefinitions()
                .values()
                .stream()
                .flatMap(td -> {
                    List<Object[]> colRes = new ArrayList<>();
                    for (int i = 0; i < td.getColumns().size(); i ++) {
                        ColumnDefinition column = td.getColumn(i);
                        colRes.add(new Object[]{
                            "def",
                            schema.getKey(),
                            td.getName(),
                            column.getName(),
                            // ordinal position
                            i + 1L,
                            // default value
                            column.getDefaultValue(),
                            // is null
                            column.isNullable() ? "YES" : "NO",
                            // type name
                            column.getTypeName(),
                            (long)column.getPrecision(),
                            null,
                            null,
                            null,
                            null,
                            "utf8",
                            "utf8_bin",
                            column.getType().toString(),
                            // is key
                            column.isPrimary() ? "PRI" : "",
                            "",
                            // privileges fix
                            "select,insert,update,references",
                            column.getComment(),
                            ""
                        });
                    }
                    return colRes.stream();
                })
        ).iterator();
    }

    private static Iterator<Object[]> getInformationPartitions(MetaService metaService) {
        Map<String, MetaService> metaServiceMap = metaService.getSubMetaServices();
        return metaServiceMap.entrySet().stream().flatMap(schema -> schema.getValue().getTableDefinitions()
            .values()
            .stream()
            .flatMap(td -> {
                if (td.getPartDefinition() == null
                    || (td.getPartDefinition() != null && td.getPartDefinition().getDetails().size() == 0)) {
                    List<Object[]> partitionList = new ArrayList<>();
                    partitionList.add(getPartitionDetail(schema.getKey(), td, null));
                    return partitionList.stream();
                } else {
                    return td.getPartDefinition()
                        .getDetails()
                        .stream()
                        .map(partDetail -> getPartitionDetail(schema.getKey(), td, partDetail))
                        .collect(Collectors.toList())
                        .stream();
                }
            })).iterator();
    }

    private static Object[] getPartitionDetail(String schemaName,
                                               TableDefinition td,
                                               PartitionDetailDefinition partDetail) {
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
            partDetail == null ? null : Arrays.toString(partDetail.getOperand()),
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
                Map<String, TableDefinition> tableDefinitionMap = e.getValue().getTableDefinitions();
                return tableDefinitionMap
                    .values()
                    .stream()
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
                            td.getTableType(),
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
                            td.getAutoIncrement(),
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
