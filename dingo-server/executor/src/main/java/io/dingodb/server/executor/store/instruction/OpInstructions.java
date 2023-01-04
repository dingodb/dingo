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
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.op.Op;
import io.dingodb.sdk.operation.result.MultiValueOpResult;
import io.dingodb.sdk.operation.unit.CollectionUnit;
import io.dingodb.server.ExecutiveRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class OpInstructions implements Instructions {

    public static final int SIZE = 16;
    public static final int COMPUTE_OC = 1;

    public static final byte id = 8;

    public static final OpInstructions INSTRUCTIONS;

    private final Processor[] processors = new Processor[SIZE];

    private final Function<byte[], Object[]>[] decoders = new Function[SIZE];

    @Override
    public void processor(int opcode, Processor processor) {
        boolean isOut = (opcode & 1) == 0;
        if ((!isOut)) {
            processors[opcode] = processor;
            return;
        }
        throw new IllegalArgumentException("Invalid processor opcode");
    }

    @Override
    public Processor processor(int opcode) {
        return processors[opcode];
    }

    @Override
    public void decoder(int opcode, Function<byte[], Object[]> decoder) {
        decoders[opcode] = decoder;
    }

    @Override
    public Function<byte[], Object[]> decoder(int opcode) {
        return decoders[opcode];
    }

    static {
        OpInstructions op = new OpInstructions();
        INSTRUCTIONS = op;

        for (short i = 0; i < SIZE; i++) {
            op.decoder(i, ProtostuffCodec::read);
        }

        // in instruction:
        op.processor(COMPUTE_OC, new Processor() {
            @Override
            public Object process(Reader reader, Writer writer, Object... operand) {
                List<byte[]> startBytes = (List<byte[]>) operand[0];
                List<byte[]> endBytes = (List<byte[]>) operand[1];
                Op head = ProtostuffCodec.read((byte[]) operand[2]);
                int timestamp = (int) operand[3];
                // todo get udf executive from executiveApi
                /*ExecutiveApi executiveApi = ApiRegistry.getDefault()
                    .proxy(ExecutiveApi.class, CoordinatorConnector.defaultConnector());*/
                io.dingodb.server.executor.store.Reader r = new io.dingodb.server.executor.store.Reader(reader);
                io.dingodb.server.executor.store.Writer w = new io.dingodb.server.executor.store.Writer(writer);
                head.context().reader(r).writer(w).startKey(startBytes).endKey(endBytes).timestamp(timestamp);

                Executive headExec = ExecutiveRegistry.getExecutive(head.execId());

                try {
                    return exec(head, headExec.execute(head.context(), null), head.context().definition);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
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
