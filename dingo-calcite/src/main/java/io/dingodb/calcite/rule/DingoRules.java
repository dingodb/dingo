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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

import java.util.List;

public final class DingoRules {
    public static final DingoAggregateReduceRule DINGO_AGGREGATE_REDUCE_RULE
        = DingoAggregateReduceRule.DEFAULT.toRule(DingoAggregateReduceRule.class);
    public static final DingoAggregateRule DINGO_AGGREGATE_RULE
        = DingoAggregateRule.DEFAULT.toRule(DingoAggregateRule.class);
    public static final DingoDistributedValuesRule DINGO_DISTRIBUTED_VALUES_RULE
        = DingoDistributedValuesRule.Config.DEFAULT.toRule();
    public static final DingoExchangeRootRule DINGO_EXCHANGE_ROOT_RULE
        = DingoExchangeRootRule.DEFAULT.toRule(DingoExchangeRootRule.class);
    public static final DingoFilterRule DINGO_FILTER_RULE_DISTRIBUTED
        = DingoFilterRule.DISTRIBUTED.toRule(DingoFilterRule.class);
    public static final DingoFilterRule DINGO_FILTER_RULE_ROOT
        = DingoFilterRule.ROOT.toRule(DingoFilterRule.class);
    public static final DingoGetByKeysRule DINGO_GET_BY_KEYS_RULE
        = DingoGetByKeysRule.Config.DEFAULT.toRule();
    public static final DingoHashJoinRootRule DINGO_HASH_JOIN_ROOT_RULE
        = DingoHashJoinRootRule.DEFAULT.toRule(DingoHashJoinRootRule.class);
    public static final DingoHashJoinRule DINGO_HASH_JOIN_RULE
        = DingoHashJoinRule.Config.DEFAULT.toRule();
    public static final DingoPartCountRule DINGO_PART_COUNT_RULE
        = DingoPartCountRule.Config.DEFAULT.toRule();
    public static final DingoPartDeleteRule DINGO_PART_DELETE_RULE
        = DingoPartDeleteRule.Config.DEFAULT.toRule();
    public static final DingoPartModifyRule DINGO_PART_MODIFY_RULE
        = DingoPartModifyRule.Config.DEFAULT.toRule();
    public static final DingoPartRangeRule DINGO_PART_RANGE_RULE
        = DingoPartRangeRule.Config.DEFAULT.toRule();
    public static final DingoPartRangeDeleteRule DINGO_PART_RANGE_DELETE_RULE
        = DingoPartRangeDeleteRule.Config.DEFAULT.toRule();
    public static final DingoProjectRule DINGO_PROJECT_RULE_DISTRIBUTED
        = DingoProjectRule.DISTRIBUTED.toRule(DingoProjectRule.class);
    public static final DingoProjectRule DINGO_PROJECT_RULE_ROOT
        = DingoProjectRule.ROOT.toRule(DingoProjectRule.class);
    public static final DingoRootRule DINGO_ROOT_RULE
        = DingoRootRule.DEFAULT.toRule(DingoRootRule.class);
    public static final DingoScanFilterRule DINGO_SCAN_FILTER_RULE
        = DingoScanFilterRule.Config.DEFAULT.toRule();
    public static final DingoScanProjectRule DINGO_SCAN_PROJECT_RULE
        = DingoScanProjectRule.Config.DEFAULT.toRule();
    public static final DingoSortRule DINGO_SORT_RULE
        = DingoSortRule.DEFAULT.toRule(DingoSortRule.class);
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
    public static final DingoValuesRule DINGO_VALUES_RULE_DISTRIBUTED
        = DingoValuesRule.DISTRIBUTED.toRule(DingoValuesRule.class);
    public static final DingoValuesRule DINGO_VALUES_RULE_ROOT
        = DingoValuesRule.ROOT.toRule(DingoValuesRule.class);
    public static final DingoValuesUnionRule DINGO_VALUES_UNION_RULE
        = DingoValuesUnionRule.Config.DEFAULT.toRule();
    public static final LogicalDingoValueRule LOGICAL_DINGO_VALUE_RULE
        = LogicalDingoValueRule.DEFAULT.toRule(LogicalDingoValueRule.class);

    private static final List<RelOptRule> rules = ImmutableList.of(
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
        //CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_EXTRACT_FILTER,
        CoreRules.PROJECT_REMOVE,
        DINGO_AGGREGATE_REDUCE_RULE,
        DINGO_AGGREGATE_RULE,
        DINGO_DISTRIBUTED_VALUES_RULE,
        DINGO_EXCHANGE_ROOT_RULE,
        DINGO_FILTER_RULE_DISTRIBUTED,
        DINGO_FILTER_RULE_ROOT,
        DINGO_GET_BY_KEYS_RULE,
        DINGO_HASH_JOIN_ROOT_RULE,
        DINGO_HASH_JOIN_RULE,
        DINGO_PART_COUNT_RULE,
        DINGO_PART_DELETE_RULE,
        DINGO_PART_MODIFY_RULE,
        DINGO_PART_RANGE_RULE,
        DINGO_PART_RANGE_DELETE_RULE,
        DINGO_PROJECT_RULE_DISTRIBUTED,
        DINGO_PROJECT_RULE_ROOT,
        DINGO_ROOT_RULE,
        DINGO_SCAN_FILTER_RULE,
        DINGO_SCAN_PROJECT_RULE,
        DINGO_SORT_RULE,
        DINGO_TABLE_SCAN_RULE,
        DINGO_UNION_RULE,
        DINGO_VALUES_COLLECT_RULE,
        DINGO_VALUES_JOIN_RULE,
        DINGO_VALUES_REDUCE_RULE_FILTER,
        DINGO_VALUES_REDUCE_RULE_PROJECT,
        DINGO_VALUES_RULE_DISTRIBUTED,
        DINGO_VALUES_RULE_ROOT,
        DINGO_VALUES_UNION_RULE,
        LOGICAL_DINGO_VALUE_RULE
    );

    private DingoRules() {
    }

    public static List<RelOptRule> rules() {
        return rules;
    }
}
