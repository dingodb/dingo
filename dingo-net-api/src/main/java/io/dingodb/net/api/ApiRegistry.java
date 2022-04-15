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

import io.dingodb.net.NetAddressProvider;

import java.lang.reflect.Method;

public interface ApiRegistry {

    <T> void register(Class<T> api, T defined);

    default <T> void register(Method method, T defined) {
        register(method.toGenericString(), method, defined);
    }

    <T> void register(String name, Method method, T defined);

    <T> T proxy(Class<T> api, NetAddressProvider addressProvider);

    <T> T proxy(Class<T> api, NetAddressProvider addressProvider, T defined);

}
