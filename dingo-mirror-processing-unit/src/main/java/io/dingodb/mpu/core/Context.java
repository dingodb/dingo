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

import io.dingodb.common.util.Optional;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;

public class Context implements io.dingodb.mpu.instruction.Context {

    protected final Core core;
    protected final Instruction instruction;

    private Reader reader;
    private Writer writer;

    public Context(Core core, Instruction instruction) {
        this.core = core;
        this.instruction = instruction;
    }

    public boolean isWrote() {
        return writer != null;
    }

    @Override
    public Reader reader() {
        if (reader != null) {
            return reader;
        }
        synchronized (this) {
            if (reader == null) {
                return reader = core.vCore.storage.reader();
            }
            return reader;
        }
    }

    @Override
    public Writer writer() {
        if (writer != null) {
            return writer;
        }
        synchronized (this) {
            if (writer == null) {
                return writer = core.vCore.storage.writer(instruction);
            }
            return null;
        }
    }

    @Override
    public Object[] operand() {
        return instruction.operand;
    }

    @Override
    public <O> O operand(int index) {
        return (O) instruction.operand[index];
    }

    @Override
    public void close() {
        synchronized (this) {
            Optional.ifPresent(reader, Reader::close);
            Optional.ifPresent(writer, Writer::close);
        }
    }
}
