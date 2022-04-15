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

package io.dingodb.server.coordinator.meta.adaptor;

import io.dingodb.server.protocol.meta.Meta;
import io.dingodb.server.protocol.meta.Stats;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MetaAdaptorRegistry {

    private static final Map<Class<? extends Meta>, Adaptor> META_ADAPTOR = new ConcurrentHashMap<>();
    private static final Map<Class<? extends Stats>, StatsAdaptor> STATS_META_ADAPTOR = new ConcurrentHashMap<>();

    public static <T extends Meta> void register(Class<T> metaClass, Adaptor<T> adaptor) {
        log.info("Register meta adaptor. {} ==> {}", metaClass.getName(), adaptor.getClass().getName());
        META_ADAPTOR.put(metaClass, adaptor);
    }

    public static <S extends Stats> void register(Class<S> statsClass, StatsAdaptor<S> metaAdaptor) {
        log.info("Register stats meta adaptor. {} ==> {}", statsClass.getName(), metaAdaptor.getClass().getName());
        STATS_META_ADAPTOR.put(statsClass, metaAdaptor);
    }

    public static <T extends Meta, A extends Adaptor<T>> A getMetaAdaptor(Class<T> metaClass) {
        return (A) META_ADAPTOR.get(metaClass);
    }

    public static <S extends Stats, A extends StatsAdaptor<S>> A getStatsMetaAdaptor(Class<S> metaClass) {
        return (A) STATS_META_ADAPTOR.get(metaClass);
    }

}
