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

package io.dingodb.calcite;

import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.calcite.traits.DingoConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.TableFunctionNamespace;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlVectorOperator;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class DingoSqlToRelConverter extends SqlToRelConverter {

    static final Config CONFIG = SqlToRelConverter.CONFIG
        .withTrimUnusedFields(true)
        .withExpand(false)
        .withInSubQueryThreshold(1000)
        // Disable simplify to use Dingo's own expr evaluation.
        .addRelBuilderConfigTransform(c -> c.withSimplify(false));

    DingoSqlToRelConverter(
        RelOptTable.ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        boolean isExplain,
        HintStrategyTable hintStrategyTable
    ) {
        super(
            viewExpander,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            CONFIG.withExplain(isExplain).withHintStrategyTable(hintStrategyTable)
        );
    }

    @Override
    protected @Nullable RexNode convertExtendedExpression(@NonNull SqlNode node, Blackboard bb) {
        // MySQL dialect
        if (node.getKind() == SqlKind.OTHER_FUNCTION) {
            SqlOperator operator = ((SqlCall) node).getOperator();
            // Override `substring` function to avoid complicated conversion in Calcite.
            if (operator.isName("substring", false)) {
                // The same of `this.rexBuilder`.
                RexBuilder rb = bb.getRexBuilder();
                List<RexNode> operands = ((SqlCall) node).getOperandList().stream()
                    .map(bb::convertExpression)
                    .collect(Collectors.toList());
                return rb.makeCall(SqlStdOperatorTable.SUBSTRING, operands);
            }
        }
        return null;
    }

    @Override
    protected void convertFrom(Blackboard bb, @Nullable SqlNode from) {
        if (from != null && from.getKind() == SqlKind.COLLECTION_TABLE) {
            convertCollectionTable(bb, (SqlCall) from);
            return;
        }
        super.convertFrom(bb, from);
    }

    @Override
    protected void convertCollectionTable(Blackboard bb, SqlCall call) {
        final SqlOperator operator = call.getOperator();
        if (!(operator instanceof SqlFunctionScanOperator) && !(operator instanceof SqlVectorOperator)) {
            super.convertCollectionTable(bb, call);
            return;
        }

        RelTraitSet traits = cluster.traitSetOf(DingoConvention.NONE);
        RexNode rexCall = bb.convertExpression(call);
        assert validator != null;
        TableFunctionNamespace namespace = (TableFunctionNamespace) validator.getNamespace(call);
        RelNode callRel = null;
        if (operator instanceof SqlFunctionScanOperator) {
            assert namespace != null;
            callRel = new DingoFunctionScan(
                cluster,
                traits,
                (RexCall) rexCall,
                namespace.getTable(),
                call.getOperandList()
            );
        } else if (operator instanceof SqlVectorOperator) {
            assert namespace != null;
            List<Object> operands = new ArrayList<>(call.getOperandList());
            callRel = new LogicalDingoVector(
                cluster,
                traits,
                (RexCall) rexCall,
                namespace.getTable(),
                operands,
                namespace.getIndex().getTableId(),
                namespace.getIndex(),
                null,
                null,
                new ArrayList<>()
            );
        }

        bb.setRoot(callRel, true);
    }
}
