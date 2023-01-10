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

import io.dingodb.common.CommonId;
import io.dingodb.server.coordinator.meta.store.MetaStoreService;
import io.dingodb.server.protocol.meta.Meta;
import io.dingodb.server.protocol.meta.Stats;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.util.DebugLog.debug;

@Slf4j
public class MetaAdaptorRegistry {

    private static final Map<Class<? extends Meta>, Adaptor> META_ADAPTOR = new ConcurrentHashMap<>();
    private static final Map<Class<? extends Stats>, StatsAdaptor> STATS_META_ADAPTOR = new ConcurrentHashMap<>();

    private static final Map<CommonId, Adaptor> META_TABLES = new ConcurrentHashMap<>();

    static {
        ServiceLoader.load(Adaptor.class).iterator().forEachRemaining(MetaAdaptorRegistry::register);
        ServiceLoader.load(StatsAdaptor.class).iterator().forEachRemaining(MetaAdaptorRegistry::register);
        META_ADAPTOR.values().forEach(MetaStoreService.STORE_SERVICE::add);
    }

    public static void reloadAllAdaptor() {
        META_ADAPTOR.values().forEach(Adaptor::reload);
        STATS_META_ADAPTOR.values().forEach(StatsAdaptor::reload);
    }

    public static <T extends Meta> void register(Adaptor<T> adaptor) {
        Class<T> adaptFor = adaptor.adaptFor();
        debug(log, "Register meta adaptor. {} ==> {}", adaptFor.getName(), adaptor.getClass().getName());
        META_ADAPTOR.put(adaptFor, adaptor);
        META_TABLES.put(adaptor.id(), adaptor);
    }

    public static <S extends Stats> void register(StatsAdaptor<S> adaptor) {
        Class<S> adaptFor = adaptor.adaptFor();
        debug(log, "Register stats meta adaptor. {} ==> {}", adaptFor.getName(), adaptor.getClass().getName());
        STATS_META_ADAPTOR.put(adaptFor, adaptor);
    }

    public static Set<CommonId> getAdaptorIds() {
        return new HashSet<>(META_TABLES.keySet());
    }

    public static Set<Adaptor> getAll() {
        return new HashSet<>(META_ADAPTOR.values());
    }

    public static <T extends Meta, A extends Adaptor<T>> A getMetaAdaptor(Class<T> metaClass) {
        return (A) META_ADAPTOR.get(metaClass);
    }

    public static <T extends Meta, A extends Adaptor<T>> A getMetaAdaptor(CommonId id) {
        return (A) META_TABLES.get(id);
    }

    public static <S extends Stats, A extends StatsAdaptor<S>> A getStatsMetaAdaptor(Class<S> metaClass) {
        return (A) STATS_META_ADAPTOR.get(metaClass);
    }

}
