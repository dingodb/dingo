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

package io.dingodb.dingokv.storage;

import com.alipay.sofa.jraft.util.BytesUtil;

import java.io.Serializable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class KVEntry implements Serializable {

    private static final long serialVersionUID = -5678680976506834026L;

    private byte[]            key;
    private byte[]            value;

    public KVEntry() {
    }

    public KVEntry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int length() {
        return (this.key == null ? 0 : this.key.length) + (this.value == null ? 0 : this.value.length);
    }

    @Override
    public String toString() {
        return "KVEntry{" + "key=" + BytesUtil.toHex(key) + ", value=" + BytesUtil.toHex(value) + '}';
    }
}
