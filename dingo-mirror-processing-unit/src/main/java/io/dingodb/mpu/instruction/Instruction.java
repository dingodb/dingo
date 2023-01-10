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
import io.dingodb.mpu.Constant;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.nio.ByteBuffer;

@ToString
@EqualsAndHashCode
public class Instruction {

    public static final int FIXED_LEN = Long.BYTES + Byte.BYTES + Short.BYTES;

    public final long clock;
    public final byte instructions;
    public final short opcode;
    public final Object[] operand;

    public Instruction(long clock, byte instructions, short opcode) {
        this(clock, instructions, opcode, new Object[0]);
    }

    public Instruction(long clock, byte instructions, short opcode, Object[] operand) {
        this.clock = clock;
        this.instructions = instructions;
        this.opcode = opcode;
        this.operand = operand;
    }

    public byte[] encode() {
        byte[] operand = ProtostuffCodec.write(this.operand);
        return ByteBuffer.allocate(FIXED_LEN + operand.length + 1)
            .put(Constant.T_INSTRUCTION).putLong(clock).put(instructions).putShort(opcode).put(operand)
            .array();
    }

    public static Instruction decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.get();
        return new Instruction(buffer.getLong(), buffer.get(), buffer.getShort(), ProtostuffCodec.read(buffer));
    }


}
