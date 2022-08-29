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
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;

import java.util.concurrent.CompletableFuture;

import static io.dingodb.mpu.instruction.InstructionSetRegistry.instructions;

class ExecutionUnit {

    public final Core core;
    public final LinkedRunner executeRunner;

    ExecutionUnit(Core core) {
        this.core = core;
        this.executeRunner = new LinkedRunner("execution-pipeline-execute-" + core.meta.label);
    }

    public void execute(Instruction instruction) {
        executeRunner.forceFollow(() -> execute0(instruction));
    }

    public <V> void execute(Instruction instruction, CompletableFuture<V> future) {
        executeRunner.forceFollow(() -> future.complete(execute0(instruction)));
    }

    private <V> V execute0(Instruction instruction) {
        Reader reader = core.storage.reader();
        Writer writer = core.storage.writer(instruction);
        V result = instructions(instruction.instructions).process(reader, writer, instruction);
        core.storage.flush(writer);
        return result;
    }
}
