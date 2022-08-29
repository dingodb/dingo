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

import java.util.function.Function;

public class ProcessingInstructions implements Instructions {

    public final int id;

    private final Processor[] processors;
    private final Function<byte[], Object[]>[] decoders;

    public ProcessingInstructions(int id) {
        this(id, Byte.MAX_VALUE);
    }

    public ProcessingInstructions(int id, int opSize) {
        this.id = id;
        processors = new Processor[opSize];
        decoders = new Function[opSize];
    }

    public synchronized void processor(int opcode, Processor processor) {
        processors[opcode] = processor;
    }

    public Processor processor(int opcode) {
        return processors[opcode];
    }

    public synchronized void decoder(int opcode, Function<byte[], Object[]> decoder) {
        decoders[opcode] = decoder;
    }

    public Function<byte[], Object[]> decoder(int opcode) {
        return decoders[opcode];
    }

}
