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

import io.dingodb.store.row.options.RpcOptions;
import io.dingodb.store.row.util.Configured;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class RpcOptionsConfigured implements Configured<RpcOptions> {
    private final RpcOptions opts;

    public static RpcOptionsConfigured newConfigured() {
        return new RpcOptionsConfigured(new RpcOptions());
    }

    public static RpcOptions newDefaultConfig() {
        return new RpcOptions();
    }

    @Override
    public RpcOptions config() {
        return this.opts;
    }

    private RpcOptionsConfigured(RpcOptions opts) {
        this.opts = opts;
    }
}

