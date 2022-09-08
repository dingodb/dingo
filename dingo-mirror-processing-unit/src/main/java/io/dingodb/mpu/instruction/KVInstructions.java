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

package io.dingodb.mpu.instruction;

import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.mpu.api.KVApi;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;

import java.util.List;
import java.util.function.Function;

import static io.dingodb.mpu.Constant.API;

public class KVInstructions implements Instructions {

    public static final int SIZE = 16;

    public static final int SET_OC = 1;
    public static final int DEL_OC = 3;
    public static final int SET_BATCH_OC = 5;
    public static final int DEL_BATCH_OC = 7;
    public static final int DEL_RANGE_OC = 9;

    public static final int COUNT_OC = 0;
    public static final int GET_OC = 2;
    public static final int SCAN_OC = 4;
    public static final int GET_BATCH_OC = 6;

    public static final KVInstructions INSTRUCTIONS;

    public interface InProcessor extends Instructions.Processor {
        @Override
        default Object process(Reader reader, Writer writer, Object... operand) {
            in(writer, operand);
            return null;
        }

        void in(Writer writer, Object... operand);
    }

    public interface OutProcessor extends Instructions.Processor {
        @Override
        default Object process(Reader reader, Writer writer, Object... operand) {
            return out(reader, operand);
        }

        Object out(Reader reader, Object... operand);
    }

    public static final byte id = 2;

    private final Instructions.Processor[] processors = new Processor[SIZE];

    private final Function<byte[], Object[]>[] decoders = new Function[SIZE];

    private KVInstructions() {
        API.register(KVApi.class, new KVApi() {});
    }

    @Override
    public void processor(int opcode, Processor processor) {
        boolean isOut = (opcode & 1) == 0;
        if ((isOut && processor instanceof OutProcessor) || (!isOut && processor instanceof InProcessor)) {
            processors[opcode] = processor;
            return;
        }
        throw new IllegalArgumentException("Invalid processor opcode.");
    }

    @Override
    public Processor processor(int opcode) {
        return processors[opcode];
    }

    @Override
    public synchronized void decoder(int opcode, Function<byte[], Object[]> decoder) {
        decoders[opcode] = decoder;
    }

    @Override
    public Function<byte[], Object[]> decoder(int opcode) {
        return decoders[opcode];
    }

    static {
        KVInstructions kv = new KVInstructions();
        INSTRUCTIONS = kv;

        // set default decoder
        for (short i = 0; i < SIZE; i++) {
            kv.decoder(i, ProtostuffCodec::read);
        }

        // in instruction:
        // set opcode 1
        kv.processor(SET_OC, (InProcessor) (writer, operand) -> writer.set((byte[]) operand[0], (byte[]) operand[1]));
        // delete opcode 3
        kv.processor(DEL_OC, (InProcessor) (writer, operand) -> writer.erase((byte[]) operand[0]));
        // set batch opcode 5
        kv.processor(SET_BATCH_OC, (InProcessor) (writer, operand) -> {
            for (int i = 0; i < operand.length; i += 2) {
                writer.set((byte[]) operand[i], (byte[]) operand[i + 1]);
            }
        });
        // delete batch opcode 7
        kv.processor(DEL_BATCH_OC, (InProcessor) (writer, operand) -> {
            for (Object o : operand) {
                writer.erase((byte[]) o);
            }
        });
        // delete range opcode 9
        kv.processor(
            DEL_RANGE_OC,
            (InProcessor) (writer, operand) -> writer.erase((byte[]) operand[0], (byte[]) operand[1])
        );

        // out instruction:
        // count opcode 0
        kv.processor(COUNT_OC, (OutProcessor) (reader, operand) -> reader.count());
        // get opcode 2
        kv.processor(GET_OC, (OutProcessor) (reader, operand) -> reader.get((byte[]) operand[0]));
        // scan opcode 4
        kv.processor(SCAN_OC, (OutProcessor) (reader, operand) -> {
            switch (operand.length) {
                case 0:
                    return reader.scan(null, null, true, true);
                case 1:
                    return reader.scan((byte[]) operand[0], null, true, true);
                case 2:
                    return reader.scan((byte[]) operand[0], (byte[]) operand[1], true, false);
                case 3:
                    return reader.scan((byte[]) operand[0], (byte[]) operand[1], true, (Boolean) operand[2]);
                default:
                    return reader
                        .scan((byte[]) operand[0], (byte[]) operand[1], (Boolean) operand[2], (Boolean) operand[3]);
            }
        });
        kv.processor(GET_BATCH_OC, (OutProcessor) (reader, operand) -> reader.get((List<byte[]>) operand[0]));

    }

}
