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

package io.dingodb.exec;

import io.dingodb.net.NetServiceProvider;
import io.dingodb.store.api.StoreServiceProvider;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.ServiceLoader;

public final class ServiceProviders<T> implements Iterable<T> {
    public static final ServiceProviders<StoreServiceProvider> KV_STORE_PROVIDER
        = new ServiceProviders<>(StoreServiceProvider.class);
    public static final ServiceProviders<StoreServiceProvider> LOCAL_STORE_PROVIDER
        = new ServiceProviders<>(StoreServiceProvider.class); // TODO
    public static final ServiceProviders<NetServiceProvider> NET_PROVIDER
        = new ServiceProviders<>(NetServiceProvider.class);

    private final ServiceLoader<T> loader;

    private ServiceProviders(Class<T> clazz) {
        this.loader = ServiceLoader.load(clazz);
    }

    public @NonNull Iterator<T> providers(boolean refresh) {
        if (refresh) {
            loader.reload();
        }
        return loader.iterator();
    }

    public @Nullable T provider(boolean refresh) {
        Iterator<T> iterator = providers(refresh);
        return iterator.hasNext() ? iterator.next() : null;
    }

    public @Nullable T provider() {
        return provider(false);
    }

    @Override
    public @NonNull Iterator<T> iterator() {
        return providers(false);
    }
}
