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

package io.dingodb.store.mpu.instruction;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.operation.Operation;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.mpu.instruction.Instructions;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class OpInstructions implements Instructions {

    public static final int SIZE = 16;
    public static final int COMPUTE_OC = 1;

    public static final byte id = 8;

    public static final OpInstructions INSTRUCTIONS;

    private final Instructions.Processor[] processors = new Processor[SIZE];

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
                byte[] start = (byte[]) operand[0];
                byte[] end = (byte[]) operand[1];
                List<Operation> operations = ((List<byte[]>) operand[2]).stream()
                    .map(ProtostuffCodec::<Operation>read)
                    .collect(Collectors.toList());
                Iterator<KeyValue> iterator = reader.scan(start, end, true, ByteArrayUtils.equal(start, end));
                for (Operation operation : operations) {
                    List<KeyValue> execute = (List<KeyValue>) operation.operationType.executive().execute(
                        operation.operationContext.startKey(start).endKey(end), iterator);
                    if (execute.size() == 0) {
                        continue;
                    }
                    iterator = execute.iterator();
                }

                int timestamp = (int) operand[3];
                if (timestamp > 0) {
                    while (iterator.hasNext()) {
                        KeyValue entry = iterator.next();
                        byte[] value = entry.getValue();
                        byte[] valueWithTs = PrimitiveCodec.encodeInt(
                            timestamp, ByteArrayUtils.unsliced(value, 0, value.length + 4), value.length, false);
                        writer.set(entry.getKey(), valueWithTs);
                    }
                } else {
                    while (iterator.hasNext()) {
                        KeyValue entry = iterator.next();
                        writer.set(entry.getKey(), entry.getValue());
                    }
                }
                return true;
            }
        });
    }
}
