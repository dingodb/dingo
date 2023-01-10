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

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;

public class SeqInstructions implements Instructions {

    public static final byte id = 3;
    public static final SeqInstructions INSTRUCTIONS = new SeqInstructions();
    private static final byte[] prefix = new byte[] {'S', 'E', 'Q'};

    private SeqInstructions() {
    }

    private final SeqIncrementProcessor<Context> processor = new SeqIncrementProcessor();

    @Override
    public Processor<Integer, Context> processor(int opcode) {
        return processor;
    }

    static class SeqIncrementProcessor<C extends Context> implements Processor<Integer, C> {
        @Override
        public Integer process(Context context) {
            byte[] key = ByteArrayUtils.concatByteArray(prefix, context.operand(0));
            int defaultSeq = 0;
            if (context.operand().length > 1) {
                defaultSeq = context.operand(1);
            }
            return Optional.ofNullable(PrimitiveCodec.decodeInt(context.reader().get(key)))
                .ifAbsentSet(defaultSeq)
                .map(seq -> seq + 1)
                .ifPresent(seq -> context.writer().set(key, PrimitiveCodec.encodeInt(seq)))
                .get();
        }
    }

}
