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

import io.dingodb.store.row.options.StoreDBOptions;
import io.dingodb.store.row.util.Configured;

public final class RocksDBOptionsConfigured implements Configured<StoreDBOptions> {
    private final StoreDBOptions opts;

    public static RocksDBOptionsConfigured newConfigured() {
        return new RocksDBOptionsConfigured(new StoreDBOptions());
    }

    public RocksDBOptionsConfigured withSync(final boolean sync) {
        this.opts.setSync(sync);
        return this;
    }

    public RocksDBOptionsConfigured withFastSnapshot(final boolean fastSnapshot) {
        this.opts.setFastSnapshot(fastSnapshot);
        return this;
    }

    public RocksDBOptionsConfigured withOpenStatisticsCollector(final boolean openStatisticsCollector) {
        this.opts.setOpenStatisticsCollector(openStatisticsCollector);
        return this;
    }

    public RocksDBOptionsConfigured withStatisticsCallbackIntervalSeconds(final long statisticsCallbackIntervalSeconds) {
        this.opts.setStatisticsCallbackIntervalSeconds(statisticsCallbackIntervalSeconds);
        return this;
    }

    public RocksDBOptionsConfigured withDbPath(final String dbPath) {
        this.opts.setDataPath(dbPath);
        return this;
    }

    @Override
    public StoreDBOptions config() {
        return this.opts;
    }

    private RocksDBOptionsConfigured(StoreDBOptions opts) {
        this.opts = opts;
    }
}
