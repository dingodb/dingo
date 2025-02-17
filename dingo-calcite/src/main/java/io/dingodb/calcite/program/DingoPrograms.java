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

package io.dingodb.calcite.program;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

public final class DingoPrograms {
    private DingoPrograms() {
    }

    public static Program subQuery(RelMetadataProvider metadataProvider) {
        HepProgramBuilder builder = HepProgram.builder();
        builder.addRuleCollection(ImmutableList.of(
            CoreRules.FILTER_SUB_QUERY_TO_CORRELATE, CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
            CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
            CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE
            //CoreRules.FILTER_INTO_JOIN
            ));
        return Programs.of(builder.build(), true, metadataProvider);
    }

}
