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

import io.dingodb.common.Location;
import io.dingodb.meta.MetaService;
import io.dingodb.server.api.MetaServiceApi;
import lombok.experimental.Delegate;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public class MetaClient extends ClientBase implements MetaService {
    @Delegate
    private MetaServiceApi metaServiceApi;

    public MetaClient(String coordinatorExchangeSvrList) {
        super(coordinatorExchangeSvrList);
    }

    @Override
    public String getName() {
        return "DINGO";
    }

    @Override
    public void init(@Nullable Map<String, Object> props) throws Exception {
        super.initConnection();
        metaServiceApi = super.getNetService()
            .apiRegistry().proxy(MetaServiceApi.class, super.getCoordinatorConnector());
    }

    @Override
    public void clear() {
    }

    @Override
    public Location currentLocation() {
        return null;
    }
}
