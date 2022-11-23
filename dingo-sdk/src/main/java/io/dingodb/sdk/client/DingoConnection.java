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


/**
 * Instantiate a connection to access a dingo database cluster.
 *
 * <p>The conneciton has two main members:
 * <br> 1.{@link MetaClient} is used to get meta from coordinator
 * <br> 2.{@link io.dingodb.net.api.ApiRegistry} is used to do operation on executor
 *
 * <p><b>When the instance is constructed, the connection MUST open first.</b>
 */
@Slf4j
public class DingoConnection extends ClientBase {
    private MetaClient metaClient;

    private ApiRegistry apiRegistry;

    /**
     * Construct connection to Dingo Cluster.
     * @param coordinatorExchangeSvrList coordinator server list.
     */
    public DingoConnection(String coordinatorExchangeSvrList) {
        super(coordinatorExchangeSvrList);
        this.metaClient = new MetaClient(coordinatorExchangeSvrList);
    }

    /**
     * when Connection is created, the open connection must be called.
     * @throws Exception exception will throw when connection open failed.
     */
    @Override
    public void initConnection() throws Exception {
        super.initConnection();
        this.metaClient.init();
        this.apiRegistry = super.getNetService().apiRegistry();
    }


    public MetaClient getMetaClient() {
        return metaClient;
    }

    public ApiRegistry getApiRegistry() {
        return apiRegistry;
    }
}
