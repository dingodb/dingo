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
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.StreamConverterLeaf;

import java.util.Collection;
import java.util.stream.Collectors;

public class DingoStreamConverterVisitFun {
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction,
        DingoTransactionRenderJob visitor, StreamConverterLeaf streamConverterLeaf) {
        Collection<Vertex> inputs = streamConverterLeaf.getData().accept(visitor);
        Collection<Vertex> outputs = inputs;
        if (transaction.getChannelMap().size() > 0) {
            outputs = outputs.stream().map(input -> {
                Location targetLocation = currentLocation;
                return DingoTransactionExchangeFun.exchange(job, idGenerator, transaction,
                    input, targetLocation, DingoTypeFactory.tuple(new DingoType[]{new BooleanType(true)}));
            }).collect(Collectors.toList());
            outputs = DingoCoalesceVisitFun.visit(job, idGenerator, currentLocation,
                transaction, visitor, outputs, streamConverterLeaf);
        }

        return outputs;
    }
}
