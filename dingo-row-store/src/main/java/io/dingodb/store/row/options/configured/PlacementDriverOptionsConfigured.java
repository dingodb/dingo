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

import com.alipay.sofa.jraft.option.CliOptions;
import io.dingodb.store.row.options.PlacementDriverOptions;
import io.dingodb.store.row.options.RegionRouteTableOptions;
import io.dingodb.store.row.options.RpcOptions;
import io.dingodb.store.row.util.Configured;

import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class PlacementDriverOptionsConfigured implements Configured<PlacementDriverOptions> {
    private final PlacementDriverOptions opts;

    public static PlacementDriverOptionsConfigured newConfigured() {
        return new PlacementDriverOptionsConfigured(new PlacementDriverOptions());
    }

    public PlacementDriverOptionsConfigured withFake(final boolean fake) {
        this.opts.setFake(fake);
        return this;
    }

    public PlacementDriverOptionsConfigured withCliOptions(final CliOptions cliOptions) {
        this.opts.setCliOptions(cliOptions);
        return this;
    }

    public PlacementDriverOptionsConfigured withPdRpcOptions(final RpcOptions pdRpcOptions) {
        this.opts.setPdRpcOptions(pdRpcOptions);
        return this;
    }

    public PlacementDriverOptionsConfigured withPdGroupId(final String pdGroupId) {
        this.opts.setPdGroupId(pdGroupId);
        return this;
    }

    public PlacementDriverOptionsConfigured withRegionRouteTableOptionsList(final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        this.opts.setRegionRouteTableOptionsList(regionRouteTableOptionsList);
        return this;
    }

    public PlacementDriverOptionsConfigured withInitialServerList(final String initialServerList) {
        this.opts.setInitialServerList(initialServerList);
        return this;
    }

    public PlacementDriverOptionsConfigured withInitialPdServerList(final String initialPdServerList) {
        this.opts.setInitialPdServerList(initialPdServerList);
        return this;
    }

    @Override
    public PlacementDriverOptions config() {
        return this.opts;
    }

    private PlacementDriverOptionsConfigured(PlacementDriverOptions opts) {
        this.opts = opts;
    }
}
