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
import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.util.Utils;
import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.Setter;
import lombok.experimental.Delegate;
import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.remote.Service;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.util.Collections;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ServiceInvocationHandler implements Service, InvocationHandler {
    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @Delegate
    private final DriverProxyApi proxyApi;
    @Setter
    private RpcMetadataResponse rpcMetadata;

    public ServiceInvocationHandler(Supplier<Location> locationSupplier, int timeout) {
        proxyApi = netService.apiRegistry().proxy(DriverProxyApi.class, locationSupplier, null, timeout);
    }

    @Override
    public Object invoke(Object proxy, @NonNull Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(this, args);
        } catch (Exception e) {
            Throwable error = Utils.extractThrowable(e);
            if (error instanceof DingoSqlException) {
                DingoSqlException dse = (DingoSqlException) error;
                throw new AvaticaClientRuntimeException(
                    dse.getMessage(),
                    dse.getSqlCode(),
                    dse.getSqlState(),
                    AvaticaSeverity.ERROR,
                    Collections.emptyList(),
                    null
                );
            }
            if (error instanceof ConnectException) {
                throw new AvaticaClientRuntimeException(
                    error.getMessage(),
                    1152,
                    "08S01",
                    AvaticaSeverity.ERROR,
                    Collections.emptyList(),
                    null
                );
            }
            throw error;
        }
    }
}
