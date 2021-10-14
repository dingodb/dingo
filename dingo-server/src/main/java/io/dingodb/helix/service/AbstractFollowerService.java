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

package io.dingodb.helix.service;

import io.dingodb.helix.state.LeaderFollowerStateModelDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.model.ExternalView;

import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractFollowerService extends AbstractStateService
    implements StateService, ExternalViewChangeListener {

    protected String leaderId;

    public AbstractFollowerService(StateServiceContext context) {
        super(context);
    }

    @Override
    public String state() {
        return LeaderFollowerStateModelDefinition.FOLLOWER;
    }

    public void onLeaderChange(String leaderId, NotificationContext changeContext) {
        log.info("Leader changed, pre leader: {}, current leader:{}", this.leaderId, leaderId);
        this.leaderId = leaderId;
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        externalViewList.stream()
            .filter(ev -> ev.getResourceName().equals(resource()))
            .findAny()
            .map(ev -> ev.getStateMap(partition()))
            .flatMap(stateMap -> stateMap.entrySet().stream()
                .filter(e -> LeaderFollowerStateModelDefinition.LEADER.equals(e.getValue()))
                .findAny())
            .map(Map.Entry::getKey)
            .filter(leaderId -> leaderId.equals(AbstractFollowerService.this.leaderId))
            .ifPresent(leaderId -> onLeaderChange(leaderId, changeContext));
    }

}
