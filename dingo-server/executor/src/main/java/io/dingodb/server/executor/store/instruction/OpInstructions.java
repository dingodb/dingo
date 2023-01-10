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

package io.dingodb.server.executor.store.instruction;

import io.dingodb.common.DingoOpResult;
import io.dingodb.common.Executive;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.mpu.instruction.Instructions;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.op.Op;
import io.dingodb.sdk.operation.result.MultiValueOpResult;
import io.dingodb.sdk.operation.unit.CollectionUnit;
import io.dingodb.server.ExecutiveRegistry;
import io.dingodb.server.executor.store.Reader;
import io.dingodb.server.executor.store.Writer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class OpInstructions implements Instructions {

    public static final int SIZE = 16;
    public static final int COMPUTE_OC = 1;

    public static final byte id = 64;

    public static final OpInstructions INSTRUCTIONS;

    private final Processor[] processors = new Processor[SIZE];

    @Override
    public Processor processor(int opcode) {
        return processors[opcode];
    }

    static {
        OpInstructions op = new OpInstructions();
        INSTRUCTIONS = op;


        // in instruction:
        op.processors[COMPUTE_OC] = context -> {
            List<byte[]> startBytes = context.operand(0);
            List<byte[]> endBytes = context.operand(1);
            Op head = ProtostuffCodec.read((byte[]) context.operand(2));
            int timestamp = context.operand(3);
            // todo get udf executive from executiveApi
            /*ExecutiveApi executiveApi = ApiRegistry.getDefault()
                .proxy(ExecutiveApi.class, CoordinatorConnector.defaultConnector());*/
            Reader r = new Reader(context.reader());
            Writer w = new Writer(context.writer());
            head.context().reader(r).writer(w).startKey(startBytes).endKey(endBytes).timestamp(timestamp);

            Executive headExec = ExecutiveRegistry.getExecutive(head.execId());

            try {
                return exec(head, headExec.execute(head.context(), null), head.context().definition);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static DingoOpResult exec(Op op, DingoOpResult result, TableDefinition definition) throws IOException {
        Op next = op.next();
        if (next == null) {
            return result;
        }
        Context preContext = op.context();
        next.context()
            .definition(definition)
            .reader(preContext.reader())
            .writer(preContext.writer())
            .startKey(preContext.startKey())
            .endKey(preContext.endKey())
            .timestamp(preContext.timestamp());
        Object record = result.getValue();
        if (result instanceof MultiValueOpResult) {
            record = ((CollectionUnit<?, ?>) record).iterator();
        }
        Executive nextExec = ExecutiveRegistry.getExecutive(next.execId());
        return exec(next, nextExec.execute(next.context(), record), definition);
    }
}
