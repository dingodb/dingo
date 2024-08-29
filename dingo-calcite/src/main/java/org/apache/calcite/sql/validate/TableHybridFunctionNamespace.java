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
import io.dingodb.calcite.DingoSqlValidator;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.common.table.HybridSearchTable;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.HybridSearchSqlUtils;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;

public class TableHybridFunctionNamespace extends AbstractNamespace {

    @Getter
    private final SqlBasicCall function;

    @Getter
    private final DingoRelOptTable table;

    @Getter
    private Table vectorIndex;

    @Getter
    private Table documentIndex;

    private SqlValidatorImpl validator;

    public TableHybridFunctionNamespace(SqlValidatorImpl validator, @Nullable SqlBasicCall enclosingNode) {
        super(validator, enclosingNode);
        assert enclosingNode != null;
        this.validator = validator;
        this.function = enclosingNode;
        ImmutableList<String> tableNames = ((SqlIdentifier)((SqlBasicCall)this.function.operand(0)).operand(0)).names;
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
        ArrayList<Column> cols = new ArrayList<>(HybridSearchTable.columns.size());

        List<SqlNode> operandList = this.function.getOperandList();

        if (function.getOperator() instanceof SqlHybridSearchOperator) {
            if (operandList.size() < 2 || operandList.size() > 4) {
                throw new RuntimeException("Incorrect parameter count for hybrid search function.");
            }
            // Get all index table definition
            Table table = dingoTable.getTable();

            List<SqlNode> documentOperandList = ((SqlBasicCall) this.function.operand(0)).getOperandList();
            String documentSelect = this.function.operand(0).toString();
            String vectorSelect = this.function.operand(1).toString();
            float documentWeight = 0.5F;
            float vectorWeight = 0.5F;
            try {
                if (operandList.size() == 3) {
                    documentWeight = ((BigDecimal) Objects.requireNonNull(((SqlNumericLiteral) operandList.get(2)).getValue())).floatValue();
                } else if (operandList.size() == 4) {
                    documentWeight = ((BigDecimal) Objects.requireNonNull(((SqlNumericLiteral) operandList.get(2)).getValue())).floatValue();
                    vectorWeight = ((BigDecimal) Objects.requireNonNull(((SqlNumericLiteral) operandList.get(3)).getValue())).floatValue();
                }
            } catch (Exception e) {
                throw new RuntimeException("The third or fourth parameter of the hybrid search function is incorrect");
            }

            if (documentOperandList.size() != 4) {
                throw new RuntimeException("Hybrid search incorrect parameter count for text search function.");
            }
            SqlIdentifier sqlIdentifier = (SqlIdentifier) documentOperandList.get(1);
            int documentLimit = 10;
            try {
                documentLimit = ((Number) Objects.requireNonNull(((SqlNumericLiteral) documentOperandList.get(3)).getValue())).intValue();
            } catch (Exception e) {
                throw new RuntimeException("The document topN parameter of the hybrid search function is incorrect");
            }
            this.documentIndex = TableFunctionNamespace.getDocumentIndexTable(table, sqlIdentifier.getSimple().toUpperCase());

            String documentId = documentIndex.getColumns().get(0).getName();
            String documentRankBm25 = documentIndex.getName() + "$rank_bm25";
            List<SqlNode> verctorOperandList = ((SqlBasicCall) this.function.operand(1)).getOperandList();
            if (verctorOperandList.size() < 4) {
                throw new RuntimeException("Hybrid search incorrect parameter count for vector function.");
            }
            SqlIdentifier columnIdentifier = (SqlIdentifier) verctorOperandList.get(1);
            int vectorLimit = 10;
            try {
                vectorLimit = ((Number) Objects.requireNonNull(((SqlNumericLiteral) verctorOperandList.get(3)).getValue())).intValue();
            } catch (Exception e) {
                throw new RuntimeException("The vector topN parameter of the hybrid search function is incorrect");
            }
            this.vectorIndex = TableFunctionNamespace.getVectorIndexTable(table, columnIdentifier.getSimple().toUpperCase());

            String vectorId = vectorIndex.getColumns().get(0).getName();
            String vectorDistance = vectorIndex.getName() + "$distance";

            cols.add(Column
                .builder()
                .name(HybridSearchTable.getColumns().get(0))
                .sqlTypeName(HybridSearchTable.TYPE_ID)
                .type(new LongType(false))
                .build()
            );

            cols.add(Column
                .builder()
                .name(HybridSearchTable.getColumns().get(1))
                .sqlTypeName(HybridSearchTable.TYPE_RANK_HYBRID)
                .type(new FloatType(false))
                .precision(-1)
                .scale(-2147483648)
                .build()
            );

            RelDataTypeFactory typeFactory = validator.typeFactory;
            RelDataType rowType = typeFactory.createStructType(
                cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
                cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
            );
            this.rowType = rowType;

            if (((DingoSqlValidator)validator).isHybridSearch()) {
                throw new RuntimeException("Multiple hybridSearch in SQL is not supported");
            }
            String sql = HybridSearchSqlUtils.hybridSearchSqlReplace(
                vectorWeight,
                documentWeight,
                vectorId,
                vectorDistance,
                vectorSelect,
                vectorLimit,
                documentId,
                documentRankBm25,
                documentSelect,
                documentLimit
            );
            ((DingoSqlValidator)validator).setHybridSearch(true);
            ((DingoSqlValidator)validator).setHybridSearchSql(sql);
            return rowType;
        } else {
            throw new RuntimeException("unsupported operator type.");
        }

    }

    @Override
    public @Nullable SqlNode getNode() {
        return function;
    }

}
