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

public interface Instructions {

    interface Processor<V, C extends Context> {
        V process(C context);
    }

    interface VoidProcessor<V, C extends Context> extends Processor<V, C> {
        @Override
        default V process(C context) {
            voidProcess(context);
            return null;
        }

        void voidProcess(C context);

    }

    <V, C extends Context> Processor<V, C> processor(int opcode);

    default <V, C extends Context> V process(int opcode, C context) {
        return this.<V, C>processor(opcode).process(context);
    }

    default <V, C extends Context> V process(Instruction instruction, C context) {
        return this.<V, C>processor(instruction.opcode).process(context);
    }

    default Function<byte[], Object[]> decoder(int opcode) {
        throw new UnsupportedOperationException();
    }

}
