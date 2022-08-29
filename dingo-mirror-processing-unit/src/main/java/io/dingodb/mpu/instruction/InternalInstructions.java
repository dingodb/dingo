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

public class InternalInstructions implements Instructions {

    public static final byte id = 0;
    public static final int SIZE = 16;

    public static final short EMPTY = 0;
    public static final InternalInstructions INSTRUCTIONS = new InternalInstructions();

    public static final Processor empty = new Processor() {
        @Override
        public <V> V process(Reader reader, Writer writer, Object... operand) {
            return null;
        }
    };

    public static final Function<byte[], Object[]> decoder = bs -> new Object[0];

    private InternalInstructions() {
    }

    @Override
    public void processor(int opcode, Processor processor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Processor processor(int opcode) {
        return empty;
    }

    @Override
    public void decoder(int opcode, Function<byte[], Object[]> decoder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Function<byte[], Object[]> decoder(int opcode) {
        return decoder;
    }
}
