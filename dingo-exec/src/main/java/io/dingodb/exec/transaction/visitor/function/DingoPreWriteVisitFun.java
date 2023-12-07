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

import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.operator.PreWriteOperator;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.PreWriteLeaf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DingoPreWriteVisitFun {
    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction, DingoTransactionRenderJob visitor, PreWriteLeaf preWriteLeaf) {
        Collection<Output> inputs = preWriteLeaf.getData().accept(visitor);
        List<Output> outputs = new ArrayList<>();
        for (Output input : inputs) {
            PreWriteOperator operator = new PreWriteOperator(new BooleanType(true), transaction.getPrimaryKey(), transaction.getStart_ts(),
                transaction.getLockTtl(), transaction.getIsolationLevel());
            Task task = input.getTask();
            operator.setId(idGenerator.getOperatorId(task.getId()));
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            operator.getSoleOutput().copyHint(input);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }
}
