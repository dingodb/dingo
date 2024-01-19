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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Column;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;

import static io.dingodb.calcite.rel.LogicalDingoTableScan.findSqlOperator;
import static io.dingodb.calcite.rel.LogicalDingoTableScan.getIndexMetricType;

public class DingoProjectRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoProjectRule"
        )
        .withRuleFactory(DingoProjectRule::new);

    protected DingoProjectRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;
        RelTraitSet traits = project.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        // for example
        // select distance(feature, array[1,2,3]) from table
        // dispatch distance to cosine/ipDistance/l2Distance
        // select name from table not use this method
        dispatchDistance(project.getProjects(), project);
        return new DingoProject(
            project.getCluster(),
            traits,
            project.getHints(),
            convert(project.getInput(), traits),
            project.getProjects(),
            project.getRowType()
        );
    }

    private static void dispatchDistance(List<RexNode> projects, LogicalProject logicalProject) {
        RelNode input = logicalProject.getInput();

        for (RexNode rexNode : projects) {
           if (rexNode instanceof RexCall && ((RexCall)rexNode).op.getName().equalsIgnoreCase("distance")) {
               RexCall rexCall = (RexCall) rexNode;
               RexNode ref = rexCall.getOperands().get(0);
               RexInputRef rexInputRef = (RexInputRef) ref;

               DingoTable dingoTable = null;
               TupleMapping tupleMapping = null;
               if (input instanceof RelSubset) {
                   RelSubset relSubset = (RelSubset) input;
                   List<RelNode> relList = relSubset.getRelList();
                   for (RelNode rel : relList) {
                       if (rel instanceof LogicalDingoTableScan) {
                           tupleMapping = ((LogicalDingoTableScan) rel).getSelection();
                           dingoTable = Objects.requireNonNull(rel.getTable()).unwrap(DingoTable.class);
                           break;
                       }
                   }
               } else if (input instanceof LogicalDingoTableScan) {
                   dingoTable = Objects.requireNonNull(input.getTable()).unwrap(DingoTable.class);
                   tupleMapping = ((LogicalDingoTableScan) input).getSelection();
               }

               int colIndex = tupleMapping.get(rexInputRef.getIndex());

               assert dingoTable != null;
               Column column = dingoTable.getTable().getColumns().get(colIndex);
               String metricType = getIndexMetricType(dingoTable, column.getName());
               SqlOperator sqlOperator1 = findSqlOperator(metricType);

               try {
                   Field field = RexCall.class.getDeclaredField("op");
                   field.setAccessible(true);
                   field.set(rexCall, sqlOperator1);
                   field.setAccessible(false);
               } catch (Exception e) {
                   throw new RuntimeException(e);
               }
           }
        }
    }

}
