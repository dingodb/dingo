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

import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.store.api.StoreServiceProvider;

import java.util.Iterator;
import java.util.ServiceLoader;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ServiceProvider<T> implements Iterable<T> {
    public static final ServiceProvider<StoreServiceProvider> KV_STORE_PROVIDER
        = new ServiceProvider<>(StoreServiceProvider.class);
    public static final ServiceProvider<MetaServiceProvider> META_PROVIDER
        = new ServiceProvider<>(MetaServiceProvider.class);
    public static final ServiceProvider<NetServiceProvider> NET_PROVIDER
        = new ServiceProvider<>(NetServiceProvider.class);

    private final ServiceLoader<T> loader;

    private ServiceProvider(Class<T> clazz) {
        this.loader = ServiceLoader.load(clazz);
    }

    @Nonnull
    public Iterator<T> providers(boolean refresh) {
        if (refresh) {
            loader.reload();
        }
        return loader.iterator();
    }

    @Nullable
    public T provider(boolean refresh) {
        Iterator<T> iterator = providers(refresh);
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Nullable
    public T provider() {
        return provider(false);
    }

    @Nonnull
    @Override
    public Iterator<T> iterator() {
        return providers(false);
    }
}
