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

package io.dingodb.helix.part;

import io.dingodb.server.ServerConfiguration;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkHelixPart extends AbstractHelixPart {
    private static final Logger logger = LoggerFactory.getLogger(ZkHelixPart.class);

    protected final String zkServer;
    protected final InstanceType type;

    public ZkHelixPart(InstanceType type, ServerConfiguration configuration) {
        super(configuration);
        this.zkServer = configuration.zkServers();
        this.type = type;
    }

    @Override
    public void init() throws Exception {
        super.init();
        manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, type, zkServer);
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

}
