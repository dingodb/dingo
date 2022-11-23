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

package io.dingodb.server.coordinator.store;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.instruction.Instructions;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;
import io.dingodb.server.protocol.CommonIdConstant;

import java.util.function.Function;

public class SeqInstructions implements Instructions {

    public static final byte id = 101;
    public static final SeqInstructions SEQ_INSTRUCTIONS = new SeqInstructions();

    private SeqInstructions() {
    }

    private final SeqIncrementProcessor processor = new SeqIncrementProcessor();

    @Override
    public void processor(int opcode, Processor processor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Processor processor(int opcode) {
        return processor;
    }

    @Override
    public void decoder(int opcode, Function<byte[], Object[]> decoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Function<byte[], Object[]> decoder(int opcode) {
        return ProtostuffCodec::read;
    }

    static class SeqIncrementProcessor implements Processor {
        private final byte[] seqPrefix = CommonId.prefix(
            CommonIdConstant.ID_TYPE.data,
            CommonIdConstant.DATA_IDENTIFIER.seq
        ).content();

        @Override
        public Object process(Reader reader, Writer writer, Object... operand) {
            byte[] realKey = ByteArrayUtils.concatByteArray(seqPrefix, (byte[]) operand[0]);
            return Optional.ofNullable(PrimitiveCodec.decodeInt(reader.get(realKey)))
                .ifAbsentSet(0)
                .map(seq -> seq + 1)
                .ifPresent(seq -> writer.set(realKey, PrimitiveCodec.encodeInt(seq)))
                .get();
        }
    }

}
