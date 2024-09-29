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

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

public class DecorrelateProgram implements Program {

    @Override
    public RelNode run(
        RelOptPlanner relOptPlanner,
        RelNode relNode,
        RelTraitSet relTraitSet,
        List<RelOptMaterialization> list,
        List<RelOptLattice> list1
    ) {
        RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(relNode.getCluster(), (RelOptSchema)null);
        return RelDecorrelator.decorrelateQuery(relNode, relBuilder);
    }
}
