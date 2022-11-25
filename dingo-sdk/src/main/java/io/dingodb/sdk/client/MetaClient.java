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
import io.dingodb.meta.MetaService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.CodeUDFApi;
import io.dingodb.server.api.MetaServiceApi;
import lombok.experimental.Delegate;

import java.util.Map;

public class MetaClient extends ClientBase implements MetaService {
    @Delegate
    private MetaServiceApi metaServiceApi;
    @Delegate
    private CodeUDFApi codeUDFApi;

    private CommonId id;

    public MetaClient(String coordinatorExchangeSvrList) {
        super(coordinatorExchangeSvrList);
    }

    @Override
    public String name() {
        return "DINGO";
    }

    public void init() throws Exception {
        super.initConnection();
        metaServiceApi = super.getNetService()
            .apiRegistry().proxy(MetaServiceApi.class, super.getCoordinatorConnector());
        codeUDFApi = ApiRegistry.getDefault().proxy(CodeUDFApi.class, super.getCoordinatorConnector());
        this.id = metaServiceApi.rootId();
    }

    @Override
    public CommonId id() {
        return this.id;
    }

    @Override
    public Map<String, MetaService> getSubMetaServices(CommonId id) {
        return null;
    }

    @Override
    public MetaService getSubMetaService(CommonId id, String name) {
        return null;
    }

    @Override
    public boolean dropSubMetaService(CommonId id, String name) {
        return false;
    }

    @Override
    public Location currentLocation() {
        return null;
    }
}
