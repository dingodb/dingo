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

package io.dingodb.driver;

import io.dingodb.common.Location;
import lombok.experimental.Delegate;
import org.apache.calcite.avatica.remote.Service;

import java.lang.reflect.Proxy;
import java.util.function.Supplier;

public class DingoServiceImpl implements Service {
    @Delegate
    private final Service proxy;

    public DingoServiceImpl(Supplier<Location> locationSupplier, int timeout) {
        proxy = (Service) Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class[]{Service.class},
            new ServiceInvocationHandler(locationSupplier, timeout)
        );
    }
}
