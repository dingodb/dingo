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

package io.dingodb.meta.local;

import com.google.auto.service.AutoService;
import io.dingodb.tso.TsoServiceProvider;

import java.util.concurrent.atomic.AtomicLong;

public class LocalTimestampOracle implements ITimestampOracle, io.dingodb.tso.TsoService {

    public static LocalTimestampOracle INSTANCE = new LocalTimestampOracle();
    private final AtomicLong localClock;
    private static int BITS_PHYSICAL_TIME = 46;
    private static int BITS_LOGICAL_TIME = 18;
    private static int LOGICAL_TIME_MASK = (1 << BITS_LOGICAL_TIME) - 1;

    public LocalTimestampOracle() {
        localClock = new AtomicLong();
    }

    public LocalTimestampOracle(final long init) {
        localClock = new AtomicLong(init);
    }

    private long next(long threshold) {
        // Make sure localClock is beyond the threshold
        long last = localClock.get();
        while (last < threshold && !localClock.compareAndSet(last, threshold)) {
            last = localClock.get();
        }
        return localClock.incrementAndGet();
    }

    @Override
    public long nextTimestamp() {
        return next(System.currentTimeMillis() << BITS_LOGICAL_TIME);
    }


    @AutoService(TsoServiceProvider.class)
    public static class LocalProvider implements TsoServiceProvider {
        @Override
        public io.dingodb.tso.TsoService get() {
            return INSTANCE;
        }
    }

    @Override
    public long tso() {
        return next(System.currentTimeMillis() << BITS_LOGICAL_TIME);
    }

    @Override
    public long tso(long timestamp) {
        return timestamp << BITS_LOGICAL_TIME;
    }

    @Override
    public long timestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public long timestamp(long tso) {
        return tso >> BITS_LOGICAL_TIME;
    }
}
