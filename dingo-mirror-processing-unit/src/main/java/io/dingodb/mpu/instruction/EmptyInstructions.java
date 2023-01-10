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

public class EmptyInstructions implements Instructions {

    public static final byte id = 0;
    public static final int SIZE = 16;

    public static final short EMPTY = 0;
    public static final EmptyInstructions INSTRUCTIONS = new EmptyInstructions();

    public static final VoidProcessor empty = __ -> { };

    private EmptyInstructions() {
    }

    @Override
    public <V, C extends Context> Processor<V, C> processor(int opcode) {
        return empty;
    }
}
