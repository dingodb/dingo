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
import io.dingodb.common.util.Utils;
import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.net.api.ApiRegistry;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.remote.Service;

import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

@Slf4j
public class DingoServiceImpl implements Service {
    @Delegate
    private final Service proxy;

    private final DriverProxyApi driverProxyApi;
    public DingoServiceImpl(Supplier<Location> locationSupplier, int timeout) {
        proxy = (Service) Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class[]{Service.class},
            new ServiceInvocationHandler(locationSupplier, timeout)
        );
        driverProxyApi = ApiRegistry.getDefault().proxy(DriverProxyApi.class, locationSupplier, timeout);
    }

    public ExecuteResponse apply(ExecuteRequest request) {
        try {
            return this.proxy.apply(request);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            Throwable throwable = Utils.extractThrowable(e);
            if (throwable instanceof TimeoutException) {
                driverProxyApi.cancel(
                    request.statementHandle.connectionId,
                    request.statementHandle.id,
                    request.statementHandle.signature
                );
            }
            throw e;
        }
    }

    public ExecuteResponse apply(PrepareAndExecuteRequest request) {
        try {
            return this.proxy.apply(request);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            Throwable throwable = Utils.extractThrowable(e);
            if (throwable instanceof TimeoutException) {
                driverProxyApi.cancel(request.connectionId, request.statementId, null);
            }
            throw e;
        }
    }

    public SyncResultsResponse apply(SyncResultsRequest request) {
        try {
            return this.proxy.apply(request);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            Throwable throwable = Utils.extractThrowable(e);
            if (throwable instanceof TimeoutException) {
                driverProxyApi.cancel(request.connectionId, request.statementId, null);
            }
            throw e;
        }
    }

    public FetchResponse apply(FetchRequest request) {
        try {
            return this.proxy.apply(request);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            Throwable throwable = Utils.extractThrowable(e);
            if (throwable instanceof TimeoutException) {
                driverProxyApi.cancel(request.connectionId, request.statementId, null);
            }
            throw e;
        }
    }

    public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
        try {
            return this.proxy.apply(request);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            Throwable throwable = Utils.extractThrowable(e);
            if (throwable instanceof TimeoutException) {
                driverProxyApi.cancel(request.connectionId, request.statementId, null);
            }
            throw e;
        }
    }

    public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
        try {
            return this.proxy.apply(request);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            Throwable throwable = Utils.extractThrowable(e);
            if (throwable instanceof TimeoutException) {
                driverProxyApi.cancel(request.connectionId, request.statementId, null);
            }
            throw e;
        }
    }

}
