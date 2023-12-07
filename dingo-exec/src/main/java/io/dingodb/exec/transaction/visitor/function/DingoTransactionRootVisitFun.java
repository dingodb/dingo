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

package io.dingodb.exec.transaction.visitor.function;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.RootLeaf;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

import static io.dingodb.common.util.Utils.sole;

public class DingoTransactionRootVisitFun {
    @NonNull
    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction, DingoTransactionRenderJob visitor, RootLeaf rootLeaf) {
        Collection<Output> inputs = rootLeaf.getData().accept(visitor);
        if (inputs.size() != 1) {
            throw new IllegalStateException("There must be one input to job root.");
        }
        Output input = sole(inputs);
        int[] mappings = new int[]{0};
        TupleMapping selection = TupleMapping.of(mappings);
        DingoType dingoType = DingoTypeFactory.tuple(new DingoType[]{new BooleanType(true)});
        Operator operator = new RootOperator(dingoType, selection);
        Task task = input.getTask();
        CommonId id = idGenerator.getOperatorId(task.getId());
        operator.setId(id);
        task.putOperator(operator);
        input.setLink(operator.getInput(0));
        task.markRoot(id);
        job.markRoot(task.getId());
        return ImmutableList.of();
    }

}
