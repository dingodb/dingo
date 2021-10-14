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

package io.dingodb.helix.rebalance;

import io.dingodb.helix.HelixAccessor;
import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.rebalance.impl.LeaderFollowerResourceAssigner;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.AbstractRebalancer;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class DingoRebalancer extends AbstractRebalancer<ResourceControllerDataProvider> {

    private final Map<HelixPart.StateMode, ResourceAssigner> resourceAssigners = new HashMap<>();
    private final ResourceAssigner defaultResourceAssigner = super::computeBestPossiblePartitionState;
    private HelixAccessor helixAccessor;

    @Override
    public void init(HelixManager manager) {
        super.init(manager);
        helixAccessor = new HelixAccessor(
            _manager.getConfigAccessor(),
            _manager.getHelixDataAccessor(),
            _manager.getClusterName()
        );
        registerResourceAssigner(HelixPart.StateMode.LEADER_FOLLOWER, new LeaderFollowerResourceAssigner(_manager));
    }

    public void registerResourceAssigner(HelixPart.StateMode stateMode, ResourceAssigner resourceAssigner) {
        resourceAssigners.put(stateMode, resourceAssigner);
    }

    @Override
    public synchronized IdealState computeNewIdealState(
        String resourceName,
        IdealState currentIdealState,
        CurrentStateOutput currentStateOutput,
        ResourceControllerDataProvider clusterData
    ) {

        List<String> instances = new ArrayList<>(
            clusterData.getEnabledLiveInstancesWithTag(currentIdealState.getInstanceGroupTag())
        );
        currentIdealState.setPreferenceLists(currentIdealState.getPartitionSet().stream().collect(toMap(
            p -> p,
            p -> instances
        )));

        return currentIdealState;
    }

    @Override
    public synchronized ResourceAssignment computeBestPossiblePartitionState(
        ResourceControllerDataProvider dataProvider,
        IdealState idealState,
        Resource resource,
        CurrentStateOutput currentStateOutput
    ) {
        return resourceAssigners.getOrDefault(
            HelixPart.StateMode.lookup(idealState.getStateModelDefRef()),
            defaultResourceAssigner
        ).computeBestPossiblePartitionState(
            dataProvider, idealState, resource, currentStateOutput
        );
    }
}
