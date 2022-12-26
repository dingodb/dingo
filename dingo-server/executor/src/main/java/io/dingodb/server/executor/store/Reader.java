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

package io.dingodb.server.executor.store;

import io.dingodb.common.store.KeyValue;

import java.util.Iterator;
import java.util.List;

public class Reader implements io.dingodb.sdk.operation.Reader {

    private final io.dingodb.mpu.storage.Reader reader;

    public Reader(io.dingodb.mpu.storage.Reader reader) {
        this.reader = reader;
    }

    @Override
    public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        return reader.scan(startKey, endKey, withStart, withEnd);
    }

    @Override
    public byte[] get(byte[] key) {
        return reader.get(key);
    }

    @Override
    public List<KeyValue> get(List<byte[]> keys) {
        return reader.get(keys);
    }
}
