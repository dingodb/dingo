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

package org.apache.calcite.sql.validate;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.DiskAnnTable;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql2rel.SqlDiskAnnOperator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;

public class TableDiskAnnFunctionNamespace extends AbstractNamespace {

    @Getter
    private final SqlBasicCall function;

    @Getter
    private final DingoRelOptTable table;

    @Getter
    private Table index;

    private SqlValidatorImpl validator;

    public TableDiskAnnFunctionNamespace(SqlValidatorImpl validator, @Nullable SqlBasicCall enclosingNode) {
        super(validator, enclosingNode);
        assert enclosingNode != null;
        this.validator = validator;
        this.function = enclosingNode;
        ImmutableList<String> tableNames = ((SqlIdentifier) this.function.operand(0)).names;
        if (tableNames.size() < 1) {
            throw DingoResource.DINGO_RESOURCE.invalidTableName("unknown").ex();
        }
        table = (DingoRelOptTable) Parameters.nonNull(
            validator.catalogReader.getTable(tableNames),
            () -> DingoResource.DINGO_RESOURCE.unknownTable(tableNames.get(tableNames.size() - 1)).ex()
        );
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        ArrayList<Column> cols = new ArrayList<>();

        List<SqlNode> operandList = this.function.getOperandList();

        if (function.getOperator() instanceof SqlDiskAnnOperator) {
            if (operandList.size() < 2) {
                throw new RuntimeException("Incorrect parameter count for disk ann vector function.");
            }
            SqlIdentifier columnIdentifier = (SqlIdentifier) operandList.get(1);
            // Get all index table definition
            Table table = dingoTable.getTable();

            this.index = getVectorIndexTable(table, columnIdentifier.getSimple().toUpperCase());
            String funcName = function.getOperator().getName();
            if (funcName.equals(DiskAnnTable.TABLE_BUILD_NAME)) {
                if (operandList.size() > 4 || operandList.size() < 2) {
                    throw new RuntimeException("Incorrect parameter count for disk_ann_build function.");
                }
                if (operandList.size() == 4) {
                    long reginId = ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandList.get(3)).getValue())).longValue();
                    MetaService metaService = MetaService.root();
                    NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges =
                        metaService.getRangeDistribution(index.tableId);
                    boolean result = indexRanges.values().stream().anyMatch(v -> v.getId().seq == reginId);
                    if (!result) {
                        throw new RuntimeException("Incorrect parameter regionId for disk_ann_build function.");
                    }
                }
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$regionId"))
                    .sqlTypeName("BIGINT")
                    .type(new LongType(false))
                    .build()
                );
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$status"))
                    .sqlTypeName("VARCHAR")
                    .type(new StringType(false))
                    .build()
                );
                RelDataTypeFactory typeFactory = validator.typeFactory;
                rowType = typeFactory.createStructType(
                    cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
                    cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
                );
            } else if (funcName.equals(DiskAnnTable.TABLE_LOAD_NAME)) {
                if (operandList.size() > 5 || operandList.size() < 2) {
                    throw new RuntimeException("Incorrect parameter count for disk_ann_load function.");
                }
                if (operandList.size() == 5) {
                    long reginId = ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandList.get(4)).getValue())).longValue();
                    MetaService metaService = MetaService.root();
                    NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges =
                        metaService.getRangeDistribution(index.tableId);
                    boolean result = indexRanges.values().stream().anyMatch(v -> v.getId().seq == reginId);
                    if (!result) {
                        throw new RuntimeException("Incorrect parameter regionId for disk_ann_load function.");
                    }
                }
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$regionId"))
                    .sqlTypeName("BIGINT")
                    .type(new LongType(false))
                    .build()
                );
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$status"))
                    .sqlTypeName("VARCHAR")
                    .type(new StringType(false))
                    .build()
                );
                RelDataTypeFactory typeFactory = validator.typeFactory;
                rowType = typeFactory.createStructType(
                    cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
                    cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
                );
            } else if (funcName.equals(DiskAnnTable.TABLE_STATUS_NAME)) {
                if (operandList.size() != 2) {
                    throw new RuntimeException("Incorrect parameter count for disk_ann_status function.");
                }
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$regionId"))
                    .sqlTypeName("BIGINT")
                    .type(new LongType(false))
                    .build()
                );
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$status"))
                    .sqlTypeName("VARCHAR")
                    .type(new StringType(false))
                    .build()
                );
                RelDataTypeFactory typeFactory = validator.typeFactory;
                rowType = typeFactory.createStructType(
                    cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
                    cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
                );
            } else if (funcName.equals(DiskAnnTable.TABLE_RESET_NAME)) {
                if (operandList.size() > 3 || operandList.size() < 2) {
                    throw new RuntimeException("Incorrect parameter count for disk_ann_reset function.");
                }
                if (operandList.size() == 3) {
                    long reginId = ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandList.get(2)).getValue())).longValue();
                    MetaService metaService = MetaService.root();
                    NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges =
                        metaService.getRangeDistribution(index.tableId);
                    boolean result = indexRanges.values().stream().anyMatch(v -> v.getId().seq == reginId);
                    if (!result) {
                        throw new RuntimeException("Incorrect parameter regionId for disk_ann_reset function.");
                    }
                }
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$regionId"))
                    .sqlTypeName("BIGINT")
                    .type(new LongType(false))
                    .build()
                );
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$status"))
                    .sqlTypeName("VARCHAR")
                    .type(new StringType(false))
                    .build()
                );
                RelDataTypeFactory typeFactory = validator.typeFactory;
                rowType = typeFactory.createStructType(
                    cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
                    cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
                );
            } else if (funcName.equals(DiskAnnTable.TABLE_COUNT_MEMORY_NAME)) {
                if (operandList.size() != 2) {
                    throw new RuntimeException("Incorrect parameter count for disk_ann_count_memory function.");
                }
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$regionId"))
                    .sqlTypeName("BIGINT")
                    .type(new LongType(false))
                    .build()
                );
                cols.add(Column
                    .builder()
                    .name(index.getName().concat("$count"))
                    .sqlTypeName("BIGINT")
                    .type(new LongType(false))
                    .build()
                );
                RelDataTypeFactory typeFactory = validator.typeFactory;
                rowType = typeFactory.createStructType(
                    cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
                    cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
                );
            } else {
                throw new RuntimeException("unsupported operator type.");
            }
            return rowType;
        } else {
            throw new RuntimeException("unsupported operator type.");
        }

    }

    @Override
    public @Nullable SqlNode getNode() {
        return function;
    }

    public static IndexTable getVectorIndexTable(Table table, String vectorColName) {
        for (IndexTable index : table.getIndexes()) {
            if (!index.getIndexType().isVector || index.indexType != IndexType.VECTOR_DISKANN ||
                !vectorColName.equalsIgnoreCase(index.getName())) {
                continue;
            }

            return index;
        }
        throw new RuntimeException(vectorColName + " disk ann vector not found.");
    }

}
