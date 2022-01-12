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

package io.dingodb.store.row.options.configured;

import io.dingodb.store.row.options.MemoryDBOptions;
import io.dingodb.store.row.util.Configured;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class MemoryDBOptionsConfigured implements Configured<MemoryDBOptions> {

    private final MemoryDBOptions opts;

    public static MemoryDBOptionsConfigured newConfigured() {
        return new MemoryDBOptionsConfigured(new MemoryDBOptions());
    }

    public MemoryDBOptionsConfigured withKeysPerSegment(final int keysPerSegment) {
        this.opts.setKeysPerSegment(keysPerSegment);
        return this;
    }

    @Override
    public MemoryDBOptions config() {
        return this.opts;
    }

    private MemoryDBOptionsConfigured(MemoryDBOptions opts) {
        this.opts = opts;
    }
}
