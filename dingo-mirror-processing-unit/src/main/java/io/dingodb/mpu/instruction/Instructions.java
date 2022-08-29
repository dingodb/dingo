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

import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;

import java.util.function.Function;

public interface Instructions {

    interface Processor {
        <V> V process(Reader reader, Writer writer, Object... operand);
    }

    void processor(int opcode, Processor processor);

    Processor processor(int opcode);

    default <V> V process(Reader reader, Writer writer, int opcode, Object... operand) {
        return processor(opcode).process(reader, writer, operand);
    }

    default <V> V process(Reader reader, Writer writer, Instruction instruction) {
        return processor(instruction.opcode).process(reader, writer, instruction.operand);
    }

    void decoder(int opcode, Function<byte[], Object[]> decoder);

    Function<byte[], Object[]> decoder(int opcode);

}
