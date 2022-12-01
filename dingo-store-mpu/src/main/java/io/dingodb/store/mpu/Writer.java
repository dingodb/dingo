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

package io.dingodb.store.mpu;

public class Writer implements io.dingodb.sdk.operation.Writer {

    private final io.dingodb.mpu.storage.Writer writer;

    public Writer(io.dingodb.mpu.storage.Writer writer) {
        this.writer = writer;
    }

    @Override
    public void set(byte[] key, byte[] value) {
        writer.set(key, value);
    }

    @Override
    public void erase(byte[] key) {
        writer.erase(key);
    }

    @Override
    public void erase(byte[] begin, byte[] end) {
        writer.erase(begin, end);
    }
}
