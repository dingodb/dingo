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

package io.dingodb.helix.state;

import io.dingodb.helix.part.HelixParticipantPart;
import io.dingodb.helix.service.LeaderFollowerServiceProvider;
import io.dingodb.helix.service.StateService;
import io.dingodb.helix.service.StateServiceContext;
import io.dingodb.helix.service.impl.StateServiceContextImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static io.dingodb.helix.state.LeaderFollowerStateModelDefinition.DROPPED;
import static io.dingodb.helix.state.LeaderFollowerStateModelDefinition.FOLLOWER;
import static io.dingodb.helix.state.LeaderFollowerStateModelDefinition.LEADER;
import static io.dingodb.helix.state.LeaderFollowerStateModelDefinition.OFFLINE;

@StateModelInfo(initialState = OFFLINE, states = {
    OFFLINE, LEADER, FOLLOWER
})
@Slf4j
public class LeaderFollowerStateModel extends AbstractStateModel<LeaderFollowerStateModel> {

    public static final int MAX_HISTORY = 8;
    private final LeaderFollowerServiceProvider serviceProvider;
    private final StateServiceContext serviceContext;
    private final List<StateService> historyServices;
    private StateService currentService;

    public LeaderFollowerStateModel(
        String partition,
        String resource,
        HelixParticipantPart<LeaderFollowerStateModel> helixParticipantPart,
        HelixPartStateModelFactory<LeaderFollowerStateModel> factory
    ) {
        super(partition, resource, helixParticipantPart, factory);
        serviceProvider = (LeaderFollowerServiceProvider) helixParticipantPart.serviceProvider();
        historyServices = new LinkedList<>();
        serviceContext = StateServiceContextImpl.builder()
            .helixPart(helixParticipantPart)
            .partition(partition)
            .stateModel(this)
            .resource(resource)
            .historyServices(historyServices)
            .build();
    }

    private void changeService(
        Function<StateServiceContext, StateService> provider,
        Message message,
        NotificationContext context
    ) throws Exception {

        currentService = provider.apply(serviceContext);
        try {
            currentService.start(message, context);
        } catch (Exception e) {
            throw new RuntimeException("Change service error.", e);
        }
        historyServices.add(currentService);
        if (historyServices.size() > MAX_HISTORY) {
            historyServices.remove(0);
        }
    }

    @Transition(from = OFFLINE, to = FOLLOWER)
    public void onBecomeReplicaFromOffline(Message message, NotificationContext context) throws Exception {
        transitionLog(OFFLINE, FOLLOWER);
        changeService(serviceProvider::replicaService, message, context);
    }

    @Transition(from = OFFLINE, to = LEADER)
    public void onBecomeLeaderFromOffline(Message message, NotificationContext context) throws Exception {
        transitionLog(OFFLINE, LEADER);
        changeService(serviceProvider::leaderService, message, context);
    }

    @Transition(from = FOLLOWER, to = OFFLINE)
    public void onBecomeOfflineFromReplica(Message message, NotificationContext context) throws Exception {
        transitionLog(FOLLOWER, OFFLINE);
        changeService(serviceProvider::offlineService, message, context);
    }

    @Transition(from = FOLLOWER, to = LEADER)
    public void onBecomeLeaderFromReplica(Message message, NotificationContext context) throws Exception {
        transitionLog(FOLLOWER, LEADER);
        changeService(serviceProvider::leaderService, message, context);
    }

    @Transition(from = LEADER, to = OFFLINE)
    public void onBecomeLeaderFromLeader(Message message, NotificationContext context) throws Exception {
        transitionLog(LEADER, OFFLINE);
        changeService(serviceProvider::offlineService, message, context);
    }

    @Transition(from = LEADER, to = FOLLOWER)
    public void onBecomeReplicaFromLeader(Message message, NotificationContext context) throws Exception {
        transitionLog(LEADER, FOLLOWER);
        changeService(serviceProvider::replicaService, message, context);
    }

    @Transition(from = OFFLINE, to = DROPPED)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) throws Exception {
        transitionLog(OFFLINE, DROPPED);
        for (StateService service : serviceContext.historyServices()) {
            if (!service.isClose()) {
                service.close();
            }
        }
    }

    public static class Factory extends HelixPartStateModelFactory<LeaderFollowerStateModel> {

        private final HelixParticipantPart<LeaderFollowerStateModel> helixParticipantPart;

        public Factory(HelixParticipantPart<LeaderFollowerStateModel> node) {
            super(node);
            this.helixParticipantPart = node;
        }

        @Override
        public LeaderFollowerStateModel createNewStateModel(String resource, String partition) {
            return new LeaderFollowerStateModel(partition, resource, helixParticipantPart, this);
        }

    }

}
