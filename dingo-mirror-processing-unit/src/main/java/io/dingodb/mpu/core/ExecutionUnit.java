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

import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.mpu.instruction.Instruction;
import lombok.extern.slf4j.Slf4j;

import static io.dingodb.mpu.instruction.InstructionSetRegistry.instructions;

@Slf4j
class ExecutionUnit {

    protected final VCore core;
    protected final LinkedRunner executeRunner;

    ExecutionUnit(VCore core) {
        this.core = core;
        this.executeRunner = new LinkedRunner("execute-" + core.meta.label);
    }

    protected void execute(Instruction instruction) {
        execute0(instruction);
    }

    protected void execute(Instruction instruction, PhaseAck ack) {
        try {
            ack.completeResult(execute0(instruction));
        } catch (Exception e) {
            ack.completeExceptionally(e);
        }
    }

    private <V> V execute0(Instruction instruction) {
        try (Context context = new Context(core.core, instruction)) {
            V result = instructions(instruction.instructions).process(instruction, context);
            if (!isInternal(instruction)) {
                core.storage.flush(context);
            }
            return result;
        }
    }

    private boolean isInternal(Instruction instruction) {
        return instruction.instructions == InternalInstructions.id;
    }

}
