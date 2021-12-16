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

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.Requires;
import io.dingodb.dingokv.options.RegionEngineOptions;
import io.dingodb.dingokv.util.Configured;
import io.dingodb.dingokv.util.Lists;
import io.dingodb.dingokv.util.Maps;

import java.util.List;
import java.util.Map;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class MultiRegionEngineOptionsConfigured implements Configured<List<RegionEngineOptions>> {

    private final Map<Long, RegionEngineOptions> optsTable;

    public static MultiRegionEngineOptionsConfigured newConfigured() {
        return new MultiRegionEngineOptionsConfigured(Maps.newHashMap());
    }

    public MultiRegionEngineOptionsConfigured withStartKey(final Long regionId, final String startKey) {
        getOrCreateOptsById(regionId).setStartKey(startKey);
        return this;
    }

    public MultiRegionEngineOptionsConfigured withStartKeyBytes(final Long regionId, final byte[] startKeyBytes) {
        getOrCreateOptsById(regionId).setStartKeyBytes(startKeyBytes);
        return this;
    }

    public MultiRegionEngineOptionsConfigured withEndKey(final Long regionId, final String endKey) {
        getOrCreateOptsById(regionId).setEndKey(endKey);
        return this;
    }

    public MultiRegionEngineOptionsConfigured withEndKeyBytes(final Long regionId, final byte[] endKeyBytes) {
        getOrCreateOptsById(regionId).setEndKeyBytes(endKeyBytes);
        return this;
    }

    public MultiRegionEngineOptionsConfigured withNodeOptions(final Long regionId, final NodeOptions nodeOptions) {
        getOrCreateOptsById(regionId).setNodeOptions(nodeOptions);
        return this;
    }

    public MultiRegionEngineOptionsConfigured withMetricsReportPeriod(final Long regionId,
                                                                      final long metricsReportPeriod) {
        getOrCreateOptsById(regionId).setMetricsReportPeriod(metricsReportPeriod);
        return this;
    }

    @Override
    public List<RegionEngineOptions> config() {
        return Lists.newArrayList(this.optsTable.values());
    }

    private RegionEngineOptions getOrCreateOptsById(final Long regionId) {
        Requires.requireNonNull(regionId, "regionId");
        RegionEngineOptions opts = this.optsTable.get(regionId);
        if (opts != null) {
            return opts;
        }
        opts = new RegionEngineOptions();
        opts.setRegionId(regionId);
        this.optsTable.put(regionId, opts);
        return opts;
    }

    private MultiRegionEngineOptionsConfigured(Map<Long, RegionEngineOptions> optsTable) {
        this.optsTable = optsTable;
    }
}
