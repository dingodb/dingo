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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.net.NetAddressProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.Setter;
import org.apache.calcite.avatica.remote.Service;

import java.util.ServiceLoader;

public class DingoServiceImpl implements Service {

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private ObjectMapper objectMapper = new ObjectMapper();

    @Setter
    private RpcMetadataResponse rpcMetadata;

    private final DriverProxyApi proxyApi;

    public DingoServiceImpl(NetAddressProvider netAddressProvider) {
        proxyApi = netService.apiRegistry().proxy(DriverProxyApi.class, netAddressProvider);
    }

    @Override
    public ResultSetResponse apply(CatalogsRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ResultSetResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSetResponse apply(SchemasRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ResultSetResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSetResponse apply(TablesRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ResultSetResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSetResponse apply(TableTypesRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ResultSetResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSetResponse apply(TypeInfoRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ResultSetResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSetResponse apply(ColumnsRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ResultSetResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PrepareResponse apply(PrepareRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), PrepareResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteResponse apply(ExecuteRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ExecuteResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteResponse apply(PrepareAndExecuteRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ExecuteResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SyncResultsResponse apply(SyncResultsRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), SyncResultsResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FetchResponse apply(FetchRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), FetchResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CreateStatementResponse apply(CreateStatementRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), CreateStatementResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseStatementResponse apply(CloseStatementRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), CloseStatementResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OpenConnectionResponse apply(OpenConnectionRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), OpenConnectionResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseConnectionResponse apply(CloseConnectionRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), CloseConnectionResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ConnectionSyncResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), DatabasePropertyResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CommitResponse apply(CommitRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), CommitResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RollbackResponse apply(RollbackRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), RollbackResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ExecuteBatchResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
        try {
            return objectMapper.readValue(proxyApi.apply(request), ExecuteBatchResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
