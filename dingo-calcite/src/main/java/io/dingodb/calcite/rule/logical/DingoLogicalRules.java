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

package io.dingodb.calcite.rule.logical;

public final class DingoLogicalRules {
    public static final LogicalMergeRelOpScanRule LOGICAL_MERGE_REL_OP_SCAN_RULE
        = LogicalMergeRelOpScanRule.Config.DEFAULT.toRule();
    public static final LogicalRelOpFromFilterRule LOGICAL_REL_OP_FROM_FILTER_RULE
        = LogicalRelOpFromFilterRule.DEFAULT.toRule(LogicalRelOpFromFilterRule.class);
    public static final LogicalRelOpFromProjectRule LOGICAL_REL_OP_FROM_PROJECT_RULE
        = LogicalRelOpFromProjectRule.DEFAULT.toRule(LogicalRelOpFromProjectRule.class);
    public static final LogicalScanWithRelOpRule LOGICAL_SCAN_WITH_REL_OP_RULE
        = LogicalScanWithRelOpRule.DEFAULT.toRule(LogicalScanWithRelOpRule.class);
    public static final LogicalSplitAggregateRule LOGICAL_SPLIT_AGGREGATE_RULE
        = LogicalSplitAggregateRule.Config.DEFAULT.toRule();

    private DingoLogicalRules() {
    }
}
