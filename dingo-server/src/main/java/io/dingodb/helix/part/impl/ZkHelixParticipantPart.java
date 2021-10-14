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

package io.dingodb.helix.part.impl;

import io.dingodb.helix.part.HelixParticipantPart;
import io.dingodb.helix.part.ZkHelixPart;
import io.dingodb.helix.service.ServiceProvider;
import io.dingodb.helix.state.PartStateModelFactoryProvider;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ZkHelixParticipantPart<M extends StateModel> extends ZkHelixPart implements HelixParticipantPart<M> {
    private static final Logger logger = LoggerFactory.getLogger(ZkHelixParticipantPart.class);

    protected final ServiceProvider<M> serviceProvider;

    protected final Map<String, Map<String, M>> stateModels;
    protected final StateModelFactory<M> stateModelFactory;

    public ZkHelixParticipantPart(
        PartStateModelFactoryProvider<M> stateModelFactory,
        ServiceProvider<M> serviceProvider,
        ServerConfiguration configuration
    ) {
        super(InstanceType.PARTICIPANT, configuration);
        stateModels = new HashMap<>();
        this.stateModelFactory = stateModelFactory.get(this);
        this.serviceProvider = serviceProvider;
    }

    @Override
    public void init() throws Exception {
        super.init();
        StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
        stateMachineEngine.registerStateModelFactory(stateMode.name, stateModelFactory);
    }

    @Override
    public M stateModel(String resource, String partition) {
        return Optional.of(stateModels.get(resource)).map(__ -> __.get(partition)).orElse(null);
    }

    @Override
    public Map<String, M> stateModel(String resource) {
        return stateModels.get(resource);
    }

    @Override
    public synchronized M stateModel(String resource, String partition, M stateModel) {
        logger.info("Add state model, resource: {}, partition: {}.", resource, partition);
        Map<String, M> partitionStateModel = stateModels.computeIfAbsent(resource, k -> new HashMap<>());
        return partitionStateModel.put(partition, stateModel);
    }

    @Override
    public ServiceProvider<M> serviceProvider() {
        return serviceProvider;
    }
}
