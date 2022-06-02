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

package io.dingodb.calcite.rel;

import io.dingodb.calcite.DingoRootSchema;
import io.dingodb.calcite.JobRunner;
import io.dingodb.calcite.MetaCache;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public final class EnumerableRoot extends SingleRel implements EnumerableRel {
    public EnumerableRoot(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Nonnull
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new EnumerableRoot(getCluster(), traitSet, sole(inputs));
    }

    @Nullable
    @Override
    public Result implement(@Nonnull EnumerableRelImplementor implementor, @Nonnull Prefer pref) {
        MetaCache.initTableDefinitions();
        PhysType physType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray()
        );
        RelNode input = getInput();
        assert input instanceof DingoRel : "The input must be DINGO_ROOT.";
        Location currentLocation = Services.metaServices.get(DingoRootSchema.DEFAULT_SCHEMA_NAME).currentLocation();
        Job job = DingoJobVisitor.createJob(input, currentLocation, true);
        // The result set would be treated as `Object` instead of `Object[]` if the result has only one column.
        String methodName = getRowType().getFieldCount() == 1 ? "runOneColumn" : "run";
        try {
            Method method = JobRunner.class.getMethod(methodName, String.class);
            return implementor.result(
                physType,
                Blocks.toBlock(
                    Expressions.call(method, Expressions.constant(job.toString()))
                )
            );
        } catch (NoSuchMethodException e) {
            log.error("Not found method [{}] in job runner.", methodName, e);
        }
        return null;
    }
}
