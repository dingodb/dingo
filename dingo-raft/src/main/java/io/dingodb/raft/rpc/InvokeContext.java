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

package io.dingodb.raft.rpc;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class InvokeContext {
    public static final String CRC_SWITCH = "invoke.crc.switch";

    private final ConcurrentMap<String, Object> ctx = new ConcurrentHashMap<>();

    public Object put(final String key, final Object value) {
        return this.ctx.put(key, value);
    }

    public Object putIfAbsent(final String key, final Object value) {
        return this.ctx.putIfAbsent(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final String key) {
        return (T) this.ctx.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T getOrDefault(final String key, final T defaultValue) {
        return (T) this.ctx.getOrDefault(key, defaultValue);
    }

    public void clear() {
        this.ctx.clear();
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return this.ctx.entrySet();
    }
}
