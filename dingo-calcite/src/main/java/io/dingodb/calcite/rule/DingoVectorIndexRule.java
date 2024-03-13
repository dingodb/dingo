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
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoGetVectorByDistance;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.calcite.rel.VectorStreamConvertor;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.LogicalDingoTableScan.dispatchDistanceCondition;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.filterIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.filterScalarIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.getScalaIndices;
import static io.dingodb.calcite.visitor.function.DingoGetVectorByDistanceVisitFun.getTargetVector;

@Slf4j
@Value.Enclosing
public class DingoVectorIndexRule extends RelRule<RelRule.Config> {

    /**
     * Creates a RelRule.
     *
     * @param config config
     */
    protected DingoVectorIndexRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        DingoVector vector = call.rel(0);
        RelNode relNode = getDingoGetVectorByDistance(vector.getFilter(), vector, false);
        if (relNode == null) {
            return;
        }
        call.transformTo(relNode);
    }

    public static RelNode getDingoGetVectorByDistance(RexNode condition, LogicalDingoVector vector, boolean forJoin) {
        DingoTable dingoTable = vector.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        TupleMapping selection = getDefaultSelection(dingoTable);

        if (condition != null) {
            dispatchDistanceCondition(condition, selection, dingoTable);
        }

        List<Float> targetVector = getTargetVector(vector.getOperands());
        // if filter matched point get by primary key, then DingoGetByKeys priority highest
        Pair<Integer, Integer> vectorIdPair = getVectorIndex(dingoTable, targetVector.size());
        assert vectorIdPair != null;
        RelTraitSet traitSet = vector.getTraitSet().replace(DingoRelStreaming.of(vector.getTable()));
        boolean preFilter = vector.getHints() != null
            && vector.getHints().size() > 0
            && "vector_pre".equalsIgnoreCase(vector.getHints().get(0).hintName);

        // vector filter match primary point get
        RelNode relNode = prePrimaryOrScalarPlan(condition, vector, vectorIdPair, traitSet, selection, preFilter);
        if (relNode != null) {
            return relNode;
        }

        if (!preFilter && !forJoin) {
            return null;
        }

        // pre filtering
        // 1. range scan operator pushdown
        // plan A: DingoTableScan pushdown -> vector get distance and sorted by vectorIds
        // plan B: DingoTableScan pushdown -> vector search by vectorIds
        // 2. vector search operator pushdown
        // plan C:table_data pushdown operator -> vector get distance and sorted by vectorIds
        // plan D: table_data pushdown operator -> vector search by vectorIds
        // 3. scalar index -> vector search by vectorIds
        // scalar operator -> vector search by vectorIds
        // scalar operator -> vector get distance and sorted by vectorIds
        DingoTableScan dingoTableScan = new DingoTableScan(vector.getCluster(),
            traitSet,
            ImmutableList.of(),
            vector.getTable(),
            condition,
            selection,
            null,
            null,
            null,
            true,
            false
            );

        VectorStreamConvertor vectorStreamConvertor = new VectorStreamConvertor(
            vector.getCluster(),
            vector.getTraitSet(),
            dingoTableScan,
            vector.getIndexTableId(),
            vectorIdPair.getKey(),
            vector.getIndexTable(),
            false);
        return new DingoGetVectorByDistance(
            vector.getCluster(),
            traitSet,
            vectorStreamConvertor,
            condition,
            vector.getTable(),
            vector.getOperands(),
            vectorIdPair.getKey(),
            vectorIdPair.getValue(),
            vector.getIndexTableId(),
            vector.getSelection(),
            vector.getIndexTable()
            );
    }

    private static DingoGetByIndex preScalarRelNode(LogicalDingoVector dingoVector,
                                         IndexValueMapSet<Integer, RexNode> indexValueMapSet,
                                         Table td,
                                         TupleMapping selection,
                                         RexNode condition) {
        Map<CommonId, Table> indexTdMap = getScalaIndices(dingoVector.getTable());

        if (indexTdMap.size() == 0) {
            return null;
        }
        Map<CommonId, Set> indexSetMap = filterScalarIndices(
            indexValueMapSet,
            indexTdMap,
            selection,
            td);
        if (indexSetMap == null) {
            return null;
        }
        if (indexSetMap.size() > 1) {
            return new DingoGetByIndexMerge(
                dingoVector.getCluster(),
                dingoVector.getTraitSet(),
                ImmutableList.of(),
                dingoVector.getTable(),
                condition,
                selection,
                false,
                indexSetMap,
                indexTdMap,
                td.keyMapping()
            );
        } else {
            return new DingoGetByIndex(
                dingoVector.getCluster(),
                dingoVector.getTraitSet(),
                ImmutableList.of(),
                dingoVector.getTable(),
                condition,
                selection,
                false,
                indexSetMap,
                indexTdMap
            );
        }
    }

    private static RelNode prePrimaryOrScalarPlan(
                                          RexNode condition,
                                          LogicalDingoVector vector,
                                          Pair<Integer, Integer> vectorIdPair,
                                          RelTraitSet traitSet,
                                          TupleMapping selection,
                                          boolean preFilter) {
        if (condition == null) {
            return null;
        }
        DingoTable dingoTable = vector.getTable().unwrap(DingoTable.class);
        RexNode rexNode = RexUtil.toDnf(vector.getCluster().getRexBuilder(), condition);
        IndexValueMapSetVisitor visitor = new IndexValueMapSetVisitor(vector.getCluster().getRexBuilder());
        IndexValueMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(visitor);
        assert dingoTable != null;
        final Table td = dingoTable.getTable();
        List<Integer> keyIndices = Arrays.stream(td.keyMapping().getMappings()).boxed().collect(Collectors.toList());

        Set<Map<Integer, RexNode>> keyMapSet = filterIndices(indexValueMapSet, keyIndices, selection);

        RelNode scan = null;
        if (keyMapSet != null) {
            scan = new DingoGetByKeys(
                vector.getCluster(),
                vector.getTraitSet(),
                ImmutableList.of(),
                vector.getTable(),
                condition,
                selection,
                keyMapSet
            );
        } else if (preFilter) {
            scan = preScalarRelNode(vector, indexValueMapSet, td, selection, condition);
        }

        if (scan == null) {
            return null;
        }
        VectorStreamConvertor vectorStreamConvertor = new VectorStreamConvertor(
            vector.getCluster(),
            vector.getTraitSet(),
            scan,
            vector.getIndexTableId(),
            vectorIdPair.getKey(),
            vector.getIndexTable(),
            false);
        DingoGetVectorByDistance dingoVectorGetDistance = new DingoGetVectorByDistance(
            vector.getCluster(),
            traitSet,
            vectorStreamConvertor,
            condition,
            vector.getTable(),
            vector.getOperands(),
            vectorIdPair.getKey(),
            vectorIdPair.getValue(),
            vector.getIndexTableId(),
            vector.getSelection(),
            vector.getIndexTable()
        );
        RelTraitSet traits = vector.getCluster().traitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        return new DingoStreamingConverter(vector.getCluster(),
            traits, dingoVectorGetDistance);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoVectorIndexRule.Config.builder()
            .description("DingoVectorIndexRule")
            .operandSupplier(b0 ->
                b0.operand(DingoVector.class).predicate(rel -> rel.getFilter() != null).noInputs()
            )
            .build();

//        Config VECTOR_JOIN = ImmutableDingoVectorIndexRule.Config.builder()
//            .description("DingoVectorJoinIndexRule")
//            .operandSupplier(
//                b0 -> b0.operand(LogicalJoin.class).predicate(rel -> {
//                    List<RelHint> hints = rel.getHints();
//                    if (hints != null && hints.size() > 0) {
//                        return "vector_pre".equalsIgnoreCase(hints.get(0).hintName);
//                    }
//                    return false;
//                }).oneInput(
//                  b1 -> b1.operand(LogicalDingoVector.class).predicate(rel -> {
//                      return rel.getFilter() == null;
//                  }).noInputs()
//                )
//            )
//            .build();

        @Override
        default DingoVectorIndexRule toRule() {
            return new DingoVectorIndexRule(this);
        }
    }

    private static Pair<Integer, Integer> getVectorIndex(DingoTable dingoTable, int dimension) {
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {

            if (!index.getIndexType().isVector) {
                continue;
            }

            int dimension1 = Integer.parseInt(index.getProperties().getProperty("dimension"));
            if (dimension != dimension1) {
                continue;
            }
            String vectorIdColName = index.getColumns().get(0).getName();
            String vectorColName = index.getColumns().get(1).getName();
            int vectorIdIndex = 0;
            int vectorIndex = 0;
            for (int i = 0; i < dingoTable.getTable().getColumns().size(); i ++) {
                Column column = dingoTable.getTable().getColumns().get(i);
                if (column.getName().equals(vectorIdColName)) {
                    vectorIdIndex = i;
                } else if (column.getName().equals(vectorColName)) {
                    vectorIndex = i;
                }
            }
            return Pair.of(vectorIdIndex, vectorIndex);
        }
        return null;
    }

    public static TupleMapping getDefaultSelection(DingoTable dingoTable) {
        int columnsCount = dingoTable.getTable().getColumns().size();
        int[] mappings = new int[columnsCount];
        for (int i = 0; i < columnsCount; i ++) {
            mappings[i] = i;
        }
        return TupleMapping.of(mappings);
    }

}
