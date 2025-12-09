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

package io.dingodb.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.type.DingoSqlTypeFactory;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Value.Enclosing
public class UnionAllAddProjectRule extends RelRule<UnionAllAddProjectRule.Config>
    implements TransformationRule, SubstitutionRule {
    protected UnionAllAddProjectRule(UnionAllAddProjectRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalUnion union = call.rel(0);
        if (union.addProject) {
            return;
        }
        RelDataType rowType = union.getRowTypeWithContext(SqlOperator.CallContext.IN_UNION);
        RelDataType input0Type = union.getInput(0).getRowType();
        boolean rowTypeCountEq = rowType.getFieldCount() == input0Type.getFieldCount();
        if (!rowTypeCountEq) {
            return;
        }
        List<RexNode> rexNodeList = new ArrayList<>();
        int fieldsCount = rowType.getFieldCount();

        int diffCnt = 0;
        for (int i = 0; i < fieldsCount; i++) {
            RelDataTypeField typeField = rowType.getFieldList().get(i);
            RelDataType diffType = null;
            List<RelDataType> relDataTypeList = new ArrayList<>();
            for (RelNode input : union.getInputs()) {
                RelDataTypeField subTypeField = input.getRowType().getFieldList().get(i);
                relDataTypeList.add(subTypeField.getType());
                if (typeField.getType() != subTypeField.getType()) {
                    diffType = subTypeField.getType();
                }
            }
            if (diffType != null) {
                RelDataType relDataType = DingoSqlTypeFactory.INSTANCE.leastRestrictiveByCast(relDataTypeList);
                RelDataType targetRelDataType = typeField.getType();
                if (relDataType == null) {
                    targetRelDataType = DingoSqlTypeFactory.INSTANCE.createSqlType(SqlTypeName.VARCHAR);
                }
                RexInputRef inputRef = new RexInputRef(i, diffType);
                List<RexNode> operands = new ArrayList<>();
                operands.add(inputRef);
                RexNode cast = new RexCall(targetRelDataType, SqlStdOperatorTable.CAST, operands);
                rexNodeList.add(cast);
                diffCnt ++;
            } else {
                rexNodeList.add(new RexInputRef(i, typeField.getType()));
            }
        }
        if (diffCnt == 0) {
            return;
        }
        try {
            RelNode logicalProject = LogicalProject.create(
                union,
                ImmutableList.of(), rexNodeList, union.getRowTypeWithContext(SqlOperator.CallContext.IN_UNION),
                ImmutableSet.of()
            );
            union.addProject = true;

            if (!union.all) {
                LogicalAggregate agg = (LogicalAggregate) RelOptUtil.createDistinctRel(logicalProject);
                call.transformTo(agg);
                return;
            }

            call.transformTo(logicalProject);
        } catch (Throwable ignored) {

        }
    }


    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        UnionAllAddProjectRule.Config DEFAULT = ImmutableUnionAllAddProjectRule.Config.builder().build()
            .withOperandFor(LogicalUnion.class);

        @Override default UnionAllAddProjectRule toRule() {
            return new UnionAllAddProjectRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default UnionAllAddProjectRule.Config withOperandFor(Class<? extends LogicalUnion> unionClass) {
            return withOperandSupplier(b0 ->
                b0.operand(unionClass).anyInputs())
                .as(UnionAllAddProjectRule.Config.class);
        }
    }
}
