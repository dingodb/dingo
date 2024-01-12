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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.ReceiveOperator;
import io.dingodb.exec.operator.SendOperator;
import io.dingodb.exec.operator.params.ReceiveParam;
import io.dingodb.exec.operator.params.SendParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import org.checkerframework.checker.nullness.qual.NonNull;

import static io.dingodb.exec.utils.OperatorCodeUtils.RECEIVE;
import static io.dingodb.exec.utils.OperatorCodeUtils.SEND;

public final class DingoTransactionExchangeFun {
    private DingoTransactionExchangeFun() {
    }

    public static Vertex exchange(
        Job job, IdGenerator idGenerator, ITransaction transaction,
        @NonNull Vertex input, @NonNull Location target, DingoType schema
    ) {
        Task task = input.getTask();
        if (target.equals(task.getLocation())) {
            return input;
        }
        CommonId id = idGenerator.getOperatorId(task.getId());
        Task rcvTask = job.getOrCreate(
            target,
            idGenerator,
            transaction.getType(),
            IsolationLevel.of(transaction.getIsolationLevel())
        );
        CommonId receiveId = idGenerator.getOperatorId(rcvTask.getId());
        SendParam sendParam = new SendParam(target.getHost(), target.getPort(), receiveId, schema);
        Vertex send = new Vertex(SEND, sendParam);
        send.setId(id);
        input.setPin(0);
        Edge edge = new Edge(input, send);
        input.addEdge(edge);
        send.addIn(edge);
        task.putVertex(send);
        ReceiveParam receiveParam = new ReceiveParam(task.getHost(), task.getLocation().getPort(), schema);
        Vertex receive = new Vertex(RECEIVE, receiveParam);
        receive.setId(receiveId);
        receive.copyHint(input);
        Edge sendEdge = new Edge(send, receive);
        send.addEdge(sendEdge);
        receive.addIn(sendEdge);
        rcvTask.putVertex(receive);
        return receive;
    }
}
