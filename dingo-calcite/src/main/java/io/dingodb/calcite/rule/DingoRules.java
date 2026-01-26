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
import io.dingodb.calcite.rule.dingo.DingoPhysicalRules;
import io.dingodb.calcite.rule.dingo.DingoRepeatUnionRule;
import io.dingodb.calcite.rule.dingo.DingoWindowRule;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;

import java.util.List;

public final class DingoRules {
    public static final DingoAggregateReduceRule DINGO_AGGREGATE_REDUCE_RULE
        = DingoAggregateReduceRule.Config.DEFAULT.toRule();
    public static final DingoAggregateRule DINGO_AGGREGATE_RULE
        = DingoAggregateRule.DEFAULT.toRule(DingoAggregateRule.class);
    public static final DingoAggregateScanRule DINGO_AGGREGATE_SCAN_RULE
        = DingoAggregateScanRule.Config.DEFAULT.toRule();
    public static final DingoFilterRule DINGO_FILTER_RULE
        = DingoFilterRule.DEFAULT.toRule(DingoFilterRule.class);
    public static final DingoGetByIndexCostRule DINGO_GET_BY_INDEX_RULE
        = DingoGetByIndexCostRule.Config.DEFAULT.toRule();
    public static final DingoLikeRule DINGO_LIKE_RULE
        = DingoLikeRule.Config.DEFAULT.toRule();
    public static final DingoPartCountRule DINGO_PART_COUNT_RULE
        = DingoPartCountRule.Config.DEFAULT.toRule();
    public static final DingoPartDeleteRule DINGO_PART_DELETE_RULE
        = DingoPartDeleteRule.Config.DEFAULT.toRule();
    public static final DingoRangeDeleteRule DINGO_PART_RANGE_DELETE_RULE
        = DingoRangeDeleteRule.Config.DEFAULT.toRule();
    public static final DingoProjectRule DINGO_PROJECT_RULE
        = DingoProjectRule.DEFAULT.toRule(DingoProjectRule.class);
    public static final DingoScanFilterRule DINGO_SCAN_FILTER_RULE
        = DingoScanFilterRule.Config.DEFAULT.toRule();
    public static final DingoForUpdateFilterRule DINGO_FOR_UPDATE_FILTER_RULE
        = DingoForUpdateFilterRule.Config.DEFAULT.toRule();
    public static final DingoScanProjectRule DINGO_SCAN_PROJECT_RULE
        = DingoScanProjectRule.Config.DEFAULT.toRule();
    public static final DingoTableModifyRule DINGO_TABLE_MODIFY_RULE
        = DingoTableModifyRule.DEFAULT.toRule(DingoTableModifyRule.class);
    public static final DingoSpecialInsertRule DINGO_SPECIAL_INSERT_RULE
        = DingoSpecialInsertRule.DEFAULT.toRule(DingoSpecialInsertRule.class);
    public static final DingoTableScanRule DINGO_TABLE_SCAN_RULE
        = DingoTableScanRule.DEFAULT.toRule(DingoTableScanRule.class);
    public static final DingoUnionRule DINGO_UNION_RULE
        = DingoUnionRule.DEFAULT.toRule(DingoUnionRule.class);
    public static final DingoValuesCollectRule DINGO_VALUES_COLLECT_RULE
        = DingoValuesCollectRule.Config.DEFAULT.toRule();
    public static final DingoValuesJoinRule DINGO_VALUES_JOIN_RULE
        = DingoValuesJoinRule.Config.DEFAULT.toRule();
    public static final DingoValuesReduceRule DINGO_VALUES_REDUCE_RULE_FILTER
        = DingoValuesReduceRule.Config.FILTER.toRule();
    public static final DingoValuesReduceRule DINGO_VALUES_REDUCE_RULE_PROJECT
        = DingoValuesReduceRule.Config.PROJECT.toRule();
    public static final DingoValuesRule DINGO_VALUES_RULE
        = DingoValuesRule.DEFAULT.toRule(DingoValuesRule.class);
    public static final DingoValuesUnionRule DINGO_VALUES_UNION_RULE
        = DingoValuesUnionRule.Config.DEFAULT.toRule();
    public static final LogicalDingoValueRule LOGICAL_DINGO_VALUE_RULE
        = LogicalDingoValueRule.DEFAULT.toRule(LogicalDingoValueRule.class);

    public static final DingoFunctionScanRule DINGO_FUNCTION_SCAN_RULE
        = DingoFunctionScanRule.DEFAULT.toRule(DingoFunctionScanRule.class);

    public static final DingoVectorIndexRule DINGO_VECTOR_INDEX_RULE
        = DingoVectorIndexRule.Config.DEFAULT.toRule();

    public static final DingoVectorJoinRule DINGO_VECTOR_JOIN_RULE
        = DingoVectorJoinRule.Config.DEFAULT.toRule();

    public static final DingoModifyIndexRule DINGO_MODIFY_INDEX_RULE
        = DingoModifyIndexRule.Config.DEFAULT.toRule();

    public static final SubQueryRemoveRule PROJECT_SUB_QUERY_TO_CORRELATE =
        SubQueryRemoveRule.Config.PROJECT.toRule();

    /**
     * Rule that converts a sub-queries from filter expressions into
     * {@link Correlate} instances.
     *
     * @see #PROJECT_SUB_QUERY_TO_CORRELATE
     * @see #JOIN_SUB_QUERY_TO_CORRELATE
     */
    public static final SubQueryRemoveRule FILTER_SUB_QUERY_TO_CORRELATE =
        SubQueryRemoveRule.Config.FILTER.toRule();

    /**
     * Rule that converts sub-queries from join expressions into
     * {@link Correlate} instances.
     *
     * @see #PROJECT_SUB_QUERY_TO_CORRELATE
     * @see #FILTER_SUB_QUERY_TO_CORRELATE
     */
    public static final SubQueryRemoveRule JOIN_SUB_QUERY_TO_CORRELATE =
        SubQueryRemoveRule.Config.JOIN.toRule();

    public static final DingoExportDataRule EXPORT_DATA_RULE =
        DingoExportDataRule.DEFAULT.toRule(DingoExportDataRule.class);

    public static final DingoForUpdateRule FOR_UPDATE_RULE =
        DingoForUpdateRule.DEFAULT.toRule(DingoForUpdateRule.class);

    public static final DingoVectorProjectRule DINGO_VECTOR_PROJECT_RULE
        = DingoVectorProjectRule.Config.DEFAULT.toRule();

    public static final DingoVectorFilterRule DINGO_VECTOR_FILTER_RULE
        = DingoVectorFilterRule.Config.DEFAULT.toRule();

    public static final DingoIndexScanMatchRule INDEX_SORT
        = DingoIndexScanMatchRule.Config.SORT.toRule();

    public static final DingoIndexScanMatchRule INDEX_PROJECT_SORT
        = DingoIndexScanMatchRule.Config.PROJECT_SORT.toRule();

    public static final DingoIndexNonLeftMatchRule INDEX_NONLEFT
        = DingoIndexNonLeftMatchRule.Config.DEFAULT.toRule();

    public static final DingoIndexScanMatchRule INDEX_NONLEFT_ORDER
        = DingoIndexScanMatchRule.Config.NONLEFT_ORDER.toRule();

    public static final DingoIndexScanMatchRule INDEX_RANGE
        = DingoIndexScanMatchRule.Config.INDEXSCAN.toRule();

    public static final DingoIndexFullScanRule INDEX_FULL_SCAN_RULE
        = DingoIndexFullScanRule.DEFAULT.toRule(DingoIndexFullScanRule.class);

    public static final DingoIndexRangeScanRule INDEX_RANGE_SCAN_RULE
        = DingoIndexRangeScanRule.DEFAULT.toRule(DingoIndexRangeScanRule.class);

    public static final DingoDocumentScanFilterRule DOCUMENT_INDEX_RANGE_SCAN_RULE
        = DingoDocumentScanFilterRule.DEFAULT.toRule(DingoDocumentScanFilterRule.class);

    public static final DingoRemoveSortRule SORT_PRIMARY_REMOVE
        = DingoRemoveSortRule.Config.REMOVE_PRIMARY_SORT.toRule();

    public static final IndexScanAggRule INDEX_SCAN_AGG
        = IndexScanAggRule.Config.INDEX_SCAN_AGG.toRule();

    public static final DingoIndexScanWithRelOpRule DINGO_INDEX_SCAN_WITH_REL_OP_RULE
        = DingoIndexScanWithRelOpRule.DEFAULT.toRule(DingoIndexScanWithRelOpRule.class);

    public static final IndexRangeMergeOpRule INDEX_RANGE_MERGE_OP_RULE
        = IndexRangeMergeOpRule.Config.INDEX_RANGE_MERGE2P.toRule();

    public static final IndexRangeFilterAggrRule INDEX_RANGE_FILTER_AGGR_RULE
        = IndexRangeFilterAggrRule.Config.INDEX_RANGE_FILTER_AGG_RULE.toRule();

    public static final IndexFullScanWithRelOpRule INDEX_FULL_SCAN_WITH_REL_OP_RULE
        = IndexFullScanWithRelOpRule.Config.INDEX_FULL_WITH_RELOP.toRule();

    public static final DingoFullScanProjectRule DINGO_FULL_SCAN_PROJECT_RULE
        = DingoFullScanProjectRule.Config.DINGO_FULL_SCAN_PROJECT_RULE.toRule();

    public static final DingoIndexCollationRule DINGO_INDEX_COLLATION_RULE
        = DingoIndexCollationRule.Config.PROJECT_SORT.toRule();

    public static final DingoIndexScanMatchRule INDEXSCAN_SORT_ASC
        = DingoIndexScanMatchRule.Config.INDEXSCAN_SORT_ORDER.toRule();

    public static final DingoTableCollationRule SORT_REMOVE_DINGO_SCAN
        = DingoTableCollationRule.Config.SORT_REMOVE_DINGO_SCAN.toRule();

    public static final DingoDocumentIndexRule DINGO_DOCUMENT_INDEX_RULE
        = DingoDocumentIndexRule.Config.DEFAULT.toRule();

    public static final DingoDocumentJoinRule DINGO_DOCUMENT_JOIN_RULE
        = DingoDocumentJoinRule.Config.DEFAULT.toRule();

    public static final DingoDocumentFilterRule DINGO_DOCUMENT_FILTER_RULE
         = DingoDocumentFilterRule.Config.DEFAULT.toRule();

    public static final  DingoDocumentProjectRule DINGO_DOCUMENT_PROJECT_RULE
        = DingoDocumentProjectRule.Config.DEFAULT.toRule();

    public static final DingoFilterReduceExpressionsRule FILTER_REDUCE_EXPRESSIONS_RULE
        = DingoFilterReduceExpressionsRule.Config.DEFAULT.toRule();
    public static final DingoWindowRule DINGO_WINDOW_RULE =
        DingoWindowRule.DEFAULT.toRule(DingoWindowRule.class);

    public static final DingoRepeatUnionRule DINGO_REPEAT_UNION_RULE =
        DingoRepeatUnionRule.DEFAULT.toRule(DingoRepeatUnionRule.class);

    public static final DingoTableSpoolRule DINGO_TABLE_SPOOL_RULE = DingoTableSpoolRule.DEFAULT_CONFIG
        .toRule(DingoTableSpoolRule.class);

    public static final DingoTableScanForSpoolRule DINGO_TABLE_SCAN_FOR_SPOOL_RULE
        = DingoTableScanForSpoolRule.DEFAULT.toRule(DingoTableScanForSpoolRule.class);

    public static final List<RelOptRule> BASE_RULES = ImmutableList.of(
        CoreRules.AGGREGATE_STAR_TABLE,
        CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
        //CalciteSystemProperty.COMMUTE.value()
        //    ? CoreRules.JOIN_ASSOCIATE
        //    : CoreRules.PROJECT_MERGE,
        CoreRules.FILTER_SCAN,
        CoreRules.PROJECT_FILTER_TRANSPOSE,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_PUSH_EXPRESSIONS,
        CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
        CoreRules.AGGREGATE_CASE_TO_FILTER,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.PROJECT_WINDOW_TRANSPOSE,
        CoreRules.MATCH,
        //JoinPushThroughJoinRule.RIGHT,
        //JoinPushThroughJoinRule.LEFT,
        CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
        CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);

    public static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES = ImmutableList.of(
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.FILTER_SET_OP_TRANSPOSE,
        AbstractConverter.ExpandConversionRule.INSTANCE,
        CoreRules.UNION_TO_DISTINCT,
        CoreRules.PROJECT_AGGREGATE_MERGE,
        CoreRules.AGGREGATE_JOIN_TRANSPOSE,
        CoreRules.AGGREGATE_MERGE,
        CoreRules.CALC_REMOVE
    );

    public static final List<RelOptRule> ABSTRACT_RULES = ImmutableList.of(
        CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
        CoreRules.UNION_PULL_UP_CONSTANTS,
        PruneEmptyRules.UNION_INSTANCE,
        PruneEmptyRules.INTERSECT_INSTANCE,
        PruneEmptyRules.MINUS_INSTANCE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
        PruneEmptyRules.EMPTY_TABLE_INSTANCE,
        CoreRules.UNION_MERGE,
        CoreRules.INTERSECT_MERGE,
        CoreRules.MINUS_MERGE,
        CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
        CoreRules.FILTER_MERGE,
        DateRangeRules.FILTER_INSTANCE,
        CoreRules.INTERSECT_TO_DISTINCT);


    public static final UnionAllAddProjectRule UNION_ALL_ADD_PROJECT_RULE = UnionAllAddProjectRule.Config.DEFAULT.toRule();

    private static final List<RelOptRule> rules = ImmutableList.of(
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        //CoreRules.JOIN_EXTRACT_FILTER,
        CoreRules.PROJECT_REMOVE,
        CoreRules.JOIN_ASSOCIATE,
        CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
        DingoPhysicalRules.DINGO_HASH_JOIN_RULE,
        DingoPhysicalRules.DINGO_ROOT_RULE,
        DingoPhysicalRules.DINGO_SORT_RULE,
        DingoPhysicalRules.DINGO_TRANSPOSE_REL_OP_STREAMING_CONVERTER_RULE,
        DINGO_GET_BY_INDEX_RULE,
        DINGO_LIKE_RULE,
        DINGO_PART_COUNT_RULE,
        DINGO_PART_DELETE_RULE,
        DINGO_PART_RANGE_DELETE_RULE,
        DINGO_SCAN_FILTER_RULE,
        DINGO_FOR_UPDATE_FILTER_RULE,
        DINGO_SCAN_PROJECT_RULE,
        DINGO_TABLE_MODIFY_RULE,
        DINGO_SPECIAL_INSERT_RULE,
        DINGO_TABLE_SCAN_RULE,
        DINGO_UNION_RULE,
        DINGO_VALUES_COLLECT_RULE,
        DINGO_VALUES_JOIN_RULE,
        DINGO_VALUES_REDUCE_RULE_FILTER,
        DINGO_VALUES_REDUCE_RULE_PROJECT,
        DINGO_VALUES_RULE,
        LOGICAL_DINGO_VALUE_RULE,
        DINGO_VALUES_UNION_RULE,
        DINGO_FUNCTION_SCAN_RULE,
        DINGO_VECTOR_INDEX_RULE,
        DINGO_VECTOR_JOIN_RULE,
        FILTER_SUB_QUERY_TO_CORRELATE,
        JOIN_SUB_QUERY_TO_CORRELATE,
        EXPORT_DATA_RULE,
        FOR_UPDATE_RULE,
        DINGO_VECTOR_PROJECT_RULE,
        DINGO_VECTOR_FILTER_RULE,
        INDEX_SORT,
        INDEX_NONLEFT,
        INDEX_FULL_SCAN_RULE,
        INDEX_RANGE,
        INDEX_RANGE_SCAN_RULE,
        SORT_PRIMARY_REMOVE,
        INDEX_SCAN_AGG,
        DINGO_INDEX_SCAN_WITH_REL_OP_RULE,
        INDEX_PROJECT_SORT,
        INDEX_RANGE_MERGE_OP_RULE,
        INDEX_RANGE_FILTER_AGGR_RULE,
        INDEX_FULL_SCAN_WITH_REL_OP_RULE,
        DINGO_FULL_SCAN_PROJECT_RULE,
        //DINGO_INDEX_COLLATION_RULE,
        INDEXSCAN_SORT_ASC,
        INDEX_NONLEFT_ORDER,
        //SORT_REMOVE_DINGO_SCAN,
        DINGO_DOCUMENT_INDEX_RULE,
        DINGO_DOCUMENT_JOIN_RULE,
        DINGO_DOCUMENT_PROJECT_RULE,
        DINGO_DOCUMENT_FILTER_RULE,
        DOCUMENT_INDEX_RANGE_SCAN_RULE,
        DINGO_WINDOW_RULE,
        DINGO_REPEAT_UNION_RULE,
        DINGO_TABLE_SPOOL_RULE,
        DINGO_TABLE_SCAN_FOR_SPOOL_RULE
    );

    private DingoRules() {
    }

    public static List<RelOptRule> rules() {
        return rules;
    }
}
