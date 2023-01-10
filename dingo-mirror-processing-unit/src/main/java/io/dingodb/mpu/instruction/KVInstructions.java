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

import io.dingodb.common.util.Parameters;

import java.util.Arrays;
import java.util.List;

public class KVInstructions implements Instructions {

    public static final byte id = 2;

    public static final int SIZE = 16;

    public static final int SET_OC = 1;
    public static final int DEL_OC = 3;
    public static final int SET_BATCH_OC = 5;
    public static final int DEL_BATCH_OC = 7;
    public static final int DEL_RANGE_OC = 9;
    public static final int DEL_RANGE_WITH_COUNT_OC = 11;

    public static final int COUNT_OC = 0;
    public static final int GET_OC = 2;
    public static final int SCAN_OC = 4;
    public static final int GET_BATCH_OC = 6;

    public static final KVInstructions INSTRUCTIONS;

    private final Instructions.Processor[] processors = new Processor[SIZE];

    public <V, C extends Context> void processor(int opcode, Processor<V, C> processor) {
        Parameters.mustNull(processors[opcode], opcode + " existed.");
        processors[opcode] = processor;
    }

    @Override
    public <V, C extends Context> Processor<V, C> processor(int opcode) {
        return processors[opcode];
    }

    static {
        KVInstructions kv = new KVInstructions();
        INSTRUCTIONS = kv;

        // in instruction:
        // set opcode 1
        kv.processors[SET_OC] = (VoidProcessor) __ -> __.writer().set(__.operand(0), __.operand(1));

        // delete opcode 3
        kv.processors[DEL_OC] = (VoidProcessor) __ -> __.writer().erase(__.operand(0));
        // set batch opcode 5
        kv.processors[SET_BATCH_OC] = (VoidProcessor) __ -> {
            for (int i = 0; i < __.operand().length; i += 2) {
                __.writer().set(__.operand(i), __.operand(i + 1));
            }
        };
        // delete batch opcode 7
        kv.processors[DEL_BATCH_OC] =
            (VoidProcessor) __ -> Arrays.stream(__.operand()).forEach(key -> __.writer().erase((byte[]) key));
        // delete range opcode 9
        kv.processors[DEL_RANGE_OC] = (VoidProcessor) __ -> __.writer().erase(__.operand(0), __.operand(1));
        // count before delete opcode 11
        kv.processors[DEL_RANGE_WITH_COUNT_OC] = context -> {
            Long count = context.reader().count(context.operand(0), context.operand(1));
            context.writer().erase(context.operand(0), context.operand(1));
            return count;
        };

        // out instruction:
        // count opcode 0
        kv.processors[COUNT_OC] = __ -> __.reader().count(__.operand(0), __.operand(1));
        // get opcode 2
        kv.processors[GET_OC] = __ -> __.reader().get((byte[]) __.operand(0));
        // scan opcode 4
        kv.processors[SCAN_OC] = __ -> {
            switch (__.operand().length) {
                case 0:
                    return __.reader().scan(null, null, true, true);
                case 1:
                    return __.reader().scan(__.operand(0), null, true, true);
                case 2:
                    return __.reader().scan(__.operand(0), __.operand(1), true, false);
                case 3:
                    return __.reader().scan(__.operand(0), __.operand(1), true, __.operand(2));
                default:
                    return __.reader().scan(__.operand(0), __.operand(1), __.operand(2), __.operand(3));
            }
        };
        kv.processors[GET_BATCH_OC] = __ -> __.reader().get((List<byte[]>) __.operand(0));
    }

}
