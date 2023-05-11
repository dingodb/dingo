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

package io.dingodb.client.mock;

import io.dingodb.common.Location;
import io.dingodb.net.Channel;
import io.dingodb.net.api.ApiRegistry;

import java.lang.reflect.Method;
import java.util.function.Supplier;

public class MockApiRegistry implements ApiRegistry {

    @Override
    public <T> void register(Class<T> api, T defined) {
    }

    @Override
    public <T> void register(String name, Method method, T defined) {

    }

    @Override
    public <T> T proxy(Class<T> api, Channel channel) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Channel channel, T defined) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Channel channel, int timeout) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Channel channel, T defined, int timeout) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, int timeout) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, T defined) {
        return null;
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, T defined, int timeout) {
        return null;
    }
}
