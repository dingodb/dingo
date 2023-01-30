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

package io.dingodb.mpu.core;

import io.dingodb.common.util.Utils;
import io.dingodb.mpu.MPURegister;
import io.dingodb.mpu.instruction.Instructions;

import java.util.concurrent.TimeUnit;

class InternalInstructions implements Instructions {

    static final byte id = 1;
    static final int SIZE = 16;

    static final int DESTROY_OC = 0;

    static final Instructions.Processor[] processors = new Instructions.Processor[SIZE];

    @Override
    public Processor processor(int opcode) {
        return processors[opcode];
    }

    static {
        processors[DESTROY_OC] = (VoidProcessor) __ -> {
            Context context = (Context) __;
            VCore vCore = context.core.vCore;
            vCore.close();
            MPURegister.unregister(vCore.meta.coreId);
            Utils.loop(() -> !vCore.viewCount.compareAndSet(0, Integer.MIN_VALUE), TimeUnit.MILLISECONDS.toNanos(10));
            vCore.storage.destroy();
        };
    }

}
