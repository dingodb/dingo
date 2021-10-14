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

package io.dingodb.helix.service.impl;

import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.service.StateService;
import io.dingodb.helix.service.StateServiceContext;
import lombok.Builder;
import org.apache.helix.participant.statemachine.StateModel;

import java.util.List;

@Builder
public class StateServiceContextImpl implements StateServiceContext {

    private List<StateService> historyServices;
    private HelixPart helixPart;
    private String resource;
    private String partition;
    private StateModel stateModel;

    @Override
    public StateService lastService() {
        if (historyServices.isEmpty()) {
            return null;
        }
        return historyServices.get(historyServices().size() - 1);
    }

    @Override
    public List<StateService> historyServices() {
        return historyServices;
    }

    @Override
    public HelixPart helixPart() {
        return helixPart;
    }

    @Override
    public String resource() {
        return resource;
    }

    @Override
    public String partition() {
        return partition;
    }

    @Override
    public Object partitionId() {
        String[] ss = partition.split("_");
        return Integer.parseInt(ss[ss.length - 1]);
    }

    @Override
    public StateModel stateModel() {
        return stateModel;
    }
}
