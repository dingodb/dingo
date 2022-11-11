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

import io.dingodb.exec.fun.string.SubstringFun;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

public class DingoSqlToRelConverter extends SqlToRelConverter {
    public static final Config CONFIG = SqlToRelConverter.CONFIG
        .withTrimUnusedFields(true)
        .withExpand(false)
        // Disable simplify to use Dingo's own expr evaluation.
        .addRelBuilderConfigTransform(c -> c.withSimplify(false));

    public DingoSqlToRelConverter(
        RelOptTable.ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        boolean isExplain
    ) {
        super(
            viewExpander,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            CONFIG.withExplain(isExplain)
        );
    }

    @Override
    protected @Nullable RexNode convertExtendedExpression(@NonNull SqlNode node, Blackboard bb) {
        // MySQL dialect
        if (node.getKind() == SqlKind.OTHER_FUNCTION) {
            SqlOperator operator = ((SqlCall) node).getOperator();
            // Override `substring` function to avoid complicated conversion in Calcite.
            if (operator.isName(SubstringFun.NAME, false)) {
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
}
