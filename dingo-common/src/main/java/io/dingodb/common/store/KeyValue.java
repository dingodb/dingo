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

package io.dingodb.common.store;

public class KeyValue extends Row {

    public KeyValue(byte[] primaryKey, byte[] raw) {
        super(primaryKey, new int[0], new int[0], new byte[][] {raw});
    }

    public void setKey(byte[] key) {
        primaryKey = key;
    }

    public void setValue(byte[] value) {
        columns[0] = value;
    }

    public byte[] getKey() {
        return primaryKey;
    }

    public byte[] getValue() {
        return columns[0];
    }

}
