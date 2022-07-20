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


import io.dingodb.net.api.ApiRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DingoConnection extends ClientBase {
    private MetaClient metaClient;

    private ApiRegistry apiRegistry;

    public DingoConnection(String configPath) {
        super(configPath);
        this.metaClient = new MetaClient(configPath);
    }

    public DingoConnection(String coordinatorExchangeSvrList,
                           String currentHost,
                           Integer currentPort) {
        super(coordinatorExchangeSvrList, currentHost, currentPort);
        this.metaClient = new MetaClient(coordinatorExchangeSvrList, currentHost, currentPort);
    }

    @Override
    public void initConnection() throws Exception {
        super.initConnection();
        this.apiRegistry = super.getNetService().apiRegistry();
    }


    public MetaClient getMetaClient() {
        return metaClient;
    }

    public ApiRegistry getApiRegistry() {
        return apiRegistry;
    }
}
