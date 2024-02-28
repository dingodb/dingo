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

package io.dingodb.calcite.rule.dingo;

public final class DingoPhysicalRules {
    public static final DingoHashJoinRule DINGO_HASH_JOIN_RULE
        = DingoHashJoinRule.DEFAULT.toRule(DingoHashJoinRule.class);
    public static final DingoReduceAggregateRule DINGO_REDUCE_AGGREGATE_RULE
        = DingoReduceAggregateRule.DEFAULT.toRule(DingoReduceAggregateRule.class);
    public static final DingoRelOpRule DINGO_REL_OP_RULE
        = DingoRelOpRule.DEFAULT.toRule(DingoRelOpRule.class);
    public static final DingoRootRule DINGO_ROOT_RULE
        = DingoRootRule.DEFAULT.toRule(DingoRootRule.class);
    public static final DingoScanWithRelOpRule DINGO_SCAN_WITH_REL_OP_RULE
        = DingoScanWithRelOpRule.DEFAULT.toRule(DingoScanWithRelOpRule.class);
    public static final DingoSortRule DINGO_SORT_RULE
        = DingoSortRule.DEFAULT.toRule(DingoSortRule.class);
    public static final DingoTransposeRelOpStreamingConverterRule DINGO_TRANSPOSE_REL_OP_STREAMING_CONVERTER_RULE
        = DingoTransposeRelOpStreamingConverterRule.Config.DEFAULT.toRule();

    private DingoPhysicalRules() {
    }
}
