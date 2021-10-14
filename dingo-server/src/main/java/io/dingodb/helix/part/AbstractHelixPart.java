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

import io.dingodb.helix.HelixAccessor;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHelixPart implements HelixPart {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHelixPart.class);

    protected final String instanceName;
    protected final String clusterName;
    protected final StateMode stateMode;

    protected final int port;

    protected HelixManager manager;

    protected HelixDataAccessor dataAccessor;
    protected ConfigAccessor configAccessor;

    protected HelixAccessor helixAccessor;

    protected ServerConfiguration configuration;

    protected boolean isActive = false;

    public AbstractHelixPart(ServerConfiguration configuration) {
        this.instanceName = configuration.instanceHost() + "_" + configuration.port();
        this.clusterName = configuration.clusterName();
        this.stateMode = StateMode.lookup(configuration.stateMode());
        this.port = configuration.port();
        this.configuration = configuration;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void start() throws Exception {
        manager.connect();
        dataAccessor = manager.getHelixDataAccessor();
        configAccessor = manager.getConfigAccessor();
        helixAccessor = new HelixAccessor(configAccessor, dataAccessor, clusterName);
        isActive = true;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down manager: " + instanceName);
            this.close();
        }));
    }

    @Override
    public void close() {
        if (!isActive) {
            return;
        }
        manager.disconnect();
    }

    public int port() {
        return port;
    }

    @Override
    public String name() {
        return instanceName;
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public ServerConfiguration configuration() {
        return configuration;
    }

    @Override
    public HelixManager helixManager() {
        return manager;
    }

    @Override
    public HelixDataAccessor dataAccessor() {
        return dataAccessor;
    }

    @Override
    public ConfigAccessor configAccessor() {
        return configAccessor;
    }

    @Override
    public HelixAccessor helixAccessor() {
        return helixAccessor;
    }
}
