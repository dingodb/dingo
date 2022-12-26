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

package io.dingodb.sdk.client;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.CodeUDFApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.meta.service.MetaServiceClient;
import lombok.experimental.Delegate;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;

public class MetaClient extends ClientBase implements MetaService {
    @Delegate
    private MetaServiceClient metaServiceClient;
    @Delegate
    private CodeUDFApi codeUDFApi;


    public MetaClient(String coordinatorExchangeSvrList) {
        super(coordinatorExchangeSvrList);
    }

    public void init() throws Exception {
        super.initConnection();
        metaServiceClient = new MetaServiceClient(getCoordinatorConnector());
        codeUDFApi = ApiRegistry.getDefault().proxy(CodeUDFApi.class, super.getCoordinatorConnector());
    }

}
