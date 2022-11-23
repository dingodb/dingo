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

package io.dingodb.net.api;

import io.dingodb.common.Location;
import io.dingodb.net.Channel;
import io.dingodb.net.NetServiceProvider;

import java.lang.reflect.Method;
import java.util.function.Supplier;

/**
 * Api registry.
 * If api parameter types have {@link io.dingodb.net.Channel}, must first parameter and set {@code null}.
 */
public interface ApiRegistry {

    static ApiRegistry getDefault() {
        return NetServiceProvider.getDefault().get().apiRegistry();
    }

    <T> void register(Class<T> api, T defined);

    default <T> void register(Method method, T defined) {
        register(method.toGenericString(), method, defined);
    }

    <T> void register(String name, Method method, T defined);

    <T> T proxy(Class<T> api, Channel channel);

    <T> T proxy(Class<T> api, Channel channel, T defined);

    <T> T proxy(Class<T> api, Channel channel, int timeout);

    <T> T proxy(Class<T> api, Channel channel, T defined, int timeout);

    <T> T proxy(Class<T> api, Supplier<Location> locationSupplier);

    <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, int timeout);

    <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, T defined);

    <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, T defined, int timeout);

    default <T> T proxy(Class<T> api, Location location) {
        return proxy(api, () -> location);
    }

    default <T> T proxy(Class<T> api, Location location, int timeout) {
        return proxy(api, () -> location, timeout);
    }

}
