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
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

import java.util.List;

public final class DingoRules {
    public static final DingoAggregateRule DINGO_AGGREGATE_RULE
        = DingoAggregateRule.Config.DEFAULT.toRule();
    public static final DingoCoalesceRule DINGO_COALESCE_RULE
        = DingoCoalesceRule.Config.DEFAULT.toRule();
    public static final DingoDistributedValuesRule DINGO_DISTRIBUTED_VALUES_RULE
        = DingoDistributedValuesRule.Config.DEFAULT.toRule();
    public static final DingoExchangeRule DINGO_EXCHANGE_ROOT_RULE
        = DingoExchangeRule.Config.DEFAULT.toRule();
    public static final DingoFilterScanRule DINGO_FILTER_TABLE_SCAN_RULE
        = DingoFilterScanRule.Config.DEFAULT.toRule();
    public static final DingoGetByKeysRule DINGO_GET_BY_KEYS_RULE
        = DingoGetByKeysRule.Config.DEFAULT.toRule();
    public static final DingoPartModifyRule DINGO_PART_MODIFY_RULE
        = DingoPartModifyRule.Config.DEFAULT.toRule();
    public static final DingoPartScanRule DINGO_PART_SCAN_RULE
        = DingoPartScanRule.Config.DEFAULT.toRule();
    public static final DingoProjectRule DINGO_PROJECT_RULE
        = DingoProjectRule.DEFAULT_CONFIG.toRule(DingoProjectRule.class);
    public static final DingoProjectScanRule DINGO_PROJECT_SCAN_RULE
        = DingoProjectScanRule.Config.DEFAULT.toRule();
    public static final DingoTableModifyRule DINGO_TABLE_MODIFY_RULE
        = DingoTableModifyRule.DEFAULT_CONFIG.toRule(DingoTableModifyRule.class);
    public static final DingoTableScanRule DINGO_TABLE_SCAN_RULE
        = DingoTableScanRule.DEFAULT_CONFIG.toRule(DingoTableScanRule.class);
    public static final DingoToEnumerableRule DINGO_TO_ENUMERABLE_RULE
        = DingoToEnumerableRule.Config.DEFAULT.toRule();
    public static final DingoValuesRule DINGO_VALUES_RULE
        = DingoValuesRule.DEFAULT_CONFIG.toRule(DingoValuesRule.class);

    private static final List<RelOptRule> rules = ImmutableList.of(
        CoreRules.PROJECT_REMOVE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE,
        EnumerableRules.ENUMERABLE_LIMIT_RULE,
        DINGO_AGGREGATE_RULE,
        DINGO_VALUES_RULE,
        DINGO_DISTRIBUTED_VALUES_RULE,
        DINGO_COALESCE_RULE,
        DINGO_FILTER_TABLE_SCAN_RULE,
        DINGO_EXCHANGE_ROOT_RULE,
        DINGO_GET_BY_KEYS_RULE,
        DINGO_PART_MODIFY_RULE,
        DINGO_PART_SCAN_RULE,
        DINGO_PROJECT_RULE,
        DINGO_PROJECT_SCAN_RULE,
        DINGO_TABLE_MODIFY_RULE,
        //DINGO_TABLE_SCAN_RULE,
        DINGO_TO_ENUMERABLE_RULE
    );

    private DingoRules() {
    }

    public static List<RelOptRule> rules() {
        return rules;
    }
}
