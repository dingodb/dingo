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

package io.dingodb.server.driver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.driver.server.ServerMetaFactory;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.LogLevelApi;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;

import java.util.Collections;
import java.util.ServiceLoader;

public class DriverProxyService implements DriverProxyApi {

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private final LocalService localService = new LocalService(new ServerMetaFactory().create(Collections.emptyList()),
        NoopMetricsSystem.getInstance());

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void start() {
        netService.apiRegistry().register(DriverProxyApi.class, this);
        netService.apiRegistry().register(LogLevelApi.class, LogLevelApi.INSTANCE);
    }

    public void stop() {
        netService.apiRegistry().register(DriverProxyApi.class, null);
    }

    @Override
    public String apply(Service.CatalogsRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.SchemasRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.TablesRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.TableTypesRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.TypeInfoRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.ColumnsRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.PrepareRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.ExecuteRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.PrepareAndExecuteRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.SyncResultsRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.FetchRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.CreateStatementRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.CloseStatementRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.OpenConnectionRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.CloseConnectionRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.ConnectionSyncRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.DatabasePropertyRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.CommitRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.RollbackRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.PrepareAndExecuteBatchRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String apply(Service.ExecuteBatchRequest request) {
        try {
            return objectMapper.writeValueAsString(localService.apply(request));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
