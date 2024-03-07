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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.rules.CoreRules;

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
    public static final DingoGetByIndexRule DINGO_GET_BY_INDEX_RULE
        = DingoGetByIndexRule.DEFAULT.toRule(DingoGetByIndexRule.class);
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
    public static final DingoScanProjectRule DINGO_SCAN_PROJECT_RULE
        = DingoScanProjectRule.Config.DEFAULT.toRule();
    public static final DingoTableModifyRule DINGO_TABLE_MODIFY_RULE
        = DingoTableModifyRule.DEFAULT.toRule(DingoTableModifyRule.class);
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

    public static final DingoWithoutPriModifyRule WITHOUT_PRI_DELETE_RULE =
        DingoWithoutPriModifyRule.Config.DELETE.toRule();

    public static final DingoWithoutPriModifyRule WITHOUT_PRI_UPDATE_RULE =
        DingoWithoutPriModifyRule.Config.UPDATE.toRule();

    public static final DingoVectorProjectRule DINGO_VECTOR_PROJECT_RULE
        = DingoVectorProjectRule.Config.DEFAULT.toRule();

    public static final DingoVectorFilterRule DINGO_VECTOR_FILTER_RULE
        = DingoVectorFilterRule.Config.DEFAULT.toRule();

    private static final List<RelOptRule> rules = ImmutableList.of(
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.JOIN_EXTRACT_FILTER,
        CoreRules.PROJECT_REMOVE,
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
        DINGO_SCAN_PROJECT_RULE,
        DINGO_TABLE_MODIFY_RULE,
        DINGO_TABLE_SCAN_RULE,
        DINGO_UNION_RULE,
        DINGO_VALUES_COLLECT_RULE,
        DINGO_VALUES_JOIN_RULE,
        DINGO_VALUES_REDUCE_RULE_FILTER,
        DINGO_VALUES_REDUCE_RULE_PROJECT,
        DINGO_VALUES_RULE,
        LOGICAL_DINGO_VALUE_RULE,
        DINGO_FUNCTION_SCAN_RULE,
        DINGO_VECTOR_INDEX_RULE,
        DINGO_VECTOR_JOIN_RULE,
        DINGO_MODIFY_INDEX_RULE,
        FILTER_SUB_QUERY_TO_CORRELATE,
        JOIN_SUB_QUERY_TO_CORRELATE,
        EXPORT_DATA_RULE,
        WITHOUT_PRI_DELETE_RULE,
        WITHOUT_PRI_UPDATE_RULE,
        DINGO_VECTOR_PROJECT_RULE,
        DINGO_VECTOR_FILTER_RULE
    );

    private DingoRules() {
    }

    public static List<RelOptRule> rules() {
        return rules;
    }
}
