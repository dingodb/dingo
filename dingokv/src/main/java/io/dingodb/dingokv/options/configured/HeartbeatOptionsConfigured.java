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

package io.dingodb.dingokv.options.configured;

import io.dingodb.dingokv.options.HeartbeatOptions;
import io.dingodb.dingokv.util.Configured;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class HeartbeatOptionsConfigured implements Configured<HeartbeatOptions> {
    private final HeartbeatOptions opts;

    public static HeartbeatOptionsConfigured newConfigured() {
        return new HeartbeatOptionsConfigured(new HeartbeatOptions());
    }

    public HeartbeatOptionsConfigured withStoreHeartbeatIntervalSeconds(final long storeHeartbeatIntervalSeconds) {
        this.opts.setStoreHeartbeatIntervalSeconds(storeHeartbeatIntervalSeconds);
        return this;
    }

    public HeartbeatOptionsConfigured withRegionHeartbeatIntervalSeconds(final long regionHeartbeatIntervalSeconds) {
        this.opts.setRegionHeartbeatIntervalSeconds(regionHeartbeatIntervalSeconds);
        return this;
    }

    public HeartbeatOptionsConfigured withHeartbeatRpcTimeoutMillis(final int heartbeatRpcTimeoutMillis) {
        this.opts.setHeartbeatRpcTimeoutMillis(heartbeatRpcTimeoutMillis);
        return this;
    }

    @Override
    public HeartbeatOptions config() {
        return this.opts;
    }

    private HeartbeatOptionsConfigured(HeartbeatOptions opts) {
        this.opts = opts;
    }
}
