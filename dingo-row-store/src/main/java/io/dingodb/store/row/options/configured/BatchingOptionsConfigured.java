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

import io.dingodb.store.row.options.BatchingOptions;
import io.dingodb.store.row.util.Configured;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class BatchingOptionsConfigured implements Configured<BatchingOptions> {
    private final BatchingOptions opts;

    public static BatchingOptionsConfigured newConfigured() {
        return new BatchingOptionsConfigured(new BatchingOptions());
    }

    public static BatchingOptions newDefaultConfig() {
        return new BatchingOptions();
    }

    public BatchingOptionsConfigured withAllowBatching(final boolean allowBatching) {
        this.opts.setAllowBatching(allowBatching);
        return this;
    }

    public BatchingOptionsConfigured withBatchSize(final int batchSize) {
        this.opts.setBatchSize(batchSize);
        return this;
    }

    public BatchingOptionsConfigured withBufSize(final int bufSize) {
        this.opts.setBufSize(bufSize);
        return this;
    }

    public BatchingOptionsConfigured withMaxWriteBytes(final int maxWriteBytes) {
        this.opts.setMaxWriteBytes(maxWriteBytes);
        return this;
    }

    public BatchingOptionsConfigured withMaxReadBytes(final int maxReadBytes) {
        this.opts.setMaxReadBytes(maxReadBytes);
        return this;
    }

    @Override
    public BatchingOptions config() {
        return this.opts;
    }

    private BatchingOptionsConfigured(BatchingOptions opts) {
        this.opts = opts;
    }
}
