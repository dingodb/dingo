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

package io.dingodb.mpu.storage.mem;

import io.dingodb.mpu.instruction.Instruction;

import java.util.HashMap;
import java.util.Map;

public class Writer implements io.dingodb.mpu.storage.Writer {

    protected Instruction instruction;
    protected Map<byte[], byte[]> cache = new HashMap<>();

    protected boolean eraseRange = false;
    protected byte[] eraseStart;
    protected byte[] eraseEnd;

    public Writer(Instruction instruction) {
        this.instruction = instruction;
    }

    @Override
    public int count() {
        return cache.size();
    }

    @Override
    public Instruction instruction() {
        return instruction;
    }

    @Override
    public void set(byte[] key, byte[] value) {
        cache.put(key, value);
    }

    @Override
    public void erase(byte[] key) {
        cache.put(key, null);
    }

    @Override
    public void erase(byte[] begin, byte[] end) {
        eraseRange = true;
        eraseStart = begin;
        eraseEnd = end;
    }

    @Override
    public void close() throws Exception {

    }
}
