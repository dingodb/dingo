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

package io.dingodb.calcite.visitor.function;

import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.ReceiveOperator;
import io.dingodb.exec.operator.SendOperator;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DingoExchangeFun {
    private DingoExchangeFun() {
    }

    public static Output exchange(
        Job job, IdGenerator idGenerator, @NonNull Output input, @NonNull Location target, DingoType schema
    ) {
        Task task = input.getTask();
        if (target.equals(task.getLocation())) {
            return input;
        }
        Id id = idGenerator.get();
        Id receiveId = idGenerator.get();
        SendOperator send = new SendOperator(target.getHost(), target.getPort(), receiveId, schema);
        send.setId(id);
        input.setLink(send.getInput(0));
        task.putOperator(send);
        ReceiveOperator receive = new ReceiveOperator(task.getHost(), task.getLocation().getPort(), schema);
        receive.setId(receiveId);
        receive.getSoleOutput().copyHint(input);
        Task rcvTask = job.getOrCreate(target, idGenerator);
        rcvTask.putOperator(receive);
        return receive.getSoleOutput();
    }
}
