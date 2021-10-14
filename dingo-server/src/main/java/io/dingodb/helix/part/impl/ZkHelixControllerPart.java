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

import io.dingodb.helix.part.HelixControllerPart;
import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.part.ZkHelixPart;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.InstanceType.CONTROLLER;
import static org.apache.helix.InstanceType.CONTROLLER_PARTICIPANT;

public class ZkHelixControllerPart extends ZkHelixPart implements HelixControllerPart {
    private static final Logger logger = LoggerFactory.getLogger(ZkHelixControllerPart.class);

    protected final ControllerMode mode;
    protected final StateModelFactory stateModelFactory;

    public ZkHelixControllerPart(ServerConfiguration configuration) {
        this(null, configuration);
    }

    public ZkHelixControllerPart(
        StateModelFactory stateModelFactory,
        ServerConfiguration configuration
    ) {
        super(
            configuration.controllerMode().equals(HelixPart.ControllerMode.STANDALONE.name()) ? CONTROLLER :
                CONTROLLER_PARTICIPANT,
            configuration
        );
        this.mode = ControllerMode.valueOf(configuration.controllerMode().toUpperCase());
        this.stateModelFactory = stateModelFactory;
    }

    @Override
    public void init() throws Exception {
        super.init();
        if (mode == ControllerMode.DISTRIBUTED) {
            if (this.stateModelFactory == null) {
                throw new RuntimeException("Controller mode is distributed, but state model factory is null!");
            }
            StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
            stateMachineEngine.registerStateModelFactory(stateMode.name, this.stateModelFactory);
        }
    }

}
