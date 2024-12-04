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

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.TsoRequest;
import io.dingodb.sdk.service.entity.meta.TsoTimestamp;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.tso.TsoServiceProvider;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import static io.dingodb.sdk.service.entity.meta.TsoOpType.OP_GEN_TSO;

public class TsoService implements io.dingodb.tso.TsoService {
    private static int threshold = 20;
    private final ConcurrentLinkedDeque<Long> cache = new ConcurrentLinkedDeque<>();
    @AutoService(TsoServiceProvider.class)
    public static class Provider implements TsoServiceProvider {
        @Override
        public io.dingodb.tso.TsoService get() {
            return INSTANCE;
        }
    }

    private static final int PHYSICAL_SHIFT = 18;
    private static final long MAX_LOGICAL = (1 << PHYSICAL_SHIFT) - 1;

    private MetaService tsoMetaService;

    public static final TsoService INSTANCE = new TsoService();

    public TsoService() {
        String coordinators = Configuration.coordinators();
        if (coordinators == null) {
            tsoMetaService = null;
            return;
        }
        this.tsoMetaService = Services.tsoService(
            Services.parse(coordinators)
        );
        getCacheTso();
    }

    public TsoService(Set<Location> coordinators) {
        setTsoMetaService(Services.tsoService(coordinators));
    }

    private void setTsoMetaService(MetaService tsoMetaService) {
        synchronized (TsoService.class) {
            this.tsoMetaService = tsoMetaService;
            if (INSTANCE.tsoMetaService == null) {
                INSTANCE.tsoMetaService = tsoMetaService;
            }
        }
    }

    public boolean isAvailable() {
        return tsoMetaService != null;
    }

    private long trace() {
        return Math.abs((((long) System.identityHashCode(this)) << 32) + System.nanoTime());
    }

    @Override
    public long tso() {
        return getLatestTso();
    }

    @Override
    public long cacheTso() {
        return getCacheTso();
    }

    @Override
    public long tso(long timestamp) {
        return timestamp << PHYSICAL_SHIFT;
    }

    @Override
    public long timestamp() {
        return tsoMetaService.tsoService(
            trace(), TsoRequest.builder().opType(OP_GEN_TSO).count(1L).build()
        ).getStartTimestamp().getPhysical();
    }

    @Override
    public long timestamp(long tso) {
        return tso >> PHYSICAL_SHIFT;
    }

    public long getLatestTso() {
        TsoTimestamp startTimestamp = tsoMetaService.tsoService(
            trace(), TsoRequest.builder().opType(OP_GEN_TSO).count(1L).build()
        ).getStartTimestamp();
        return (startTimestamp.getPhysical() << PHYSICAL_SHIFT) + (startTimestamp.getLogical() & MAX_LOGICAL);
    }


    public long getCacheTso() {
        Long tso = cache.pollLast();
        if (tso == null) {
            synchronized (this) {
                if (cache.isEmpty()) {
                    loadBatchTso();
                }
                tso = cache.pollLast();
            }
        }
        return tso;
    }

    private void loadBatchTso() {
        TsoTimestamp startTimestamp = tsoMetaService.tsoService(
            trace(), TsoRequest.builder().opType(OP_GEN_TSO).count(threshold).build()
        ).getStartTimestamp();
        long physical = startTimestamp.getPhysical();
        for (int i = 0; i < threshold; i++) {
            cache.addFirst((physical << PHYSICAL_SHIFT) + ((startTimestamp.getLogical() + i) & MAX_LOGICAL));
        }
    }

}
