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
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.participant.statemachine.StateModel;

@Slf4j
public abstract class AbstractStateModel<M extends AbstractStateModel<M>> extends StateModel {

    protected final String name;
    protected final String partition;
    protected final String resource;
    protected final HelixManager manager;

    protected final PropertyKey.Builder keyBuilder;

    protected final HelixParticipantPart<M> helixParticipantPart;

    protected final HelixPartStateModelFactory<M> factory;

    public AbstractStateModel(
        String partition,
        String resource,
        HelixParticipantPart<M> helixParticipantPart,
        HelixPartStateModelFactory<M> factory
    ) {
        this.partition = partition;
        this.manager = helixParticipantPart.helixManager();
        this.resource = resource;
        this.helixParticipantPart = helixParticipantPart;
        this.factory = factory;
        name = helixParticipantPart.name();
        keyBuilder = new PropertyKey.Builder(manager.getClusterName());
        helixParticipantPart.stateModel(resource, partition, (M) this);
    }

    protected void transitionLog(String from, String to) {
        log.info(
            "Resource [{}] partition [{}] transition from [{}] to [{}].",
            resource, partition, from, to
        );
    }

    public String name() {
        return name;
    }

    public String partition() {
        return partition;
    }

    public String resource() {
        return resource;
    }

    public HelixManager manager() {
        return manager;
    }

    public PropertyKey.Builder keyBuilder() {
        return keyBuilder;
    }

    public HelixParticipantPart<M> helixParticipantPart() {
        return helixParticipantPart;
    }

    public HelixPartStateModelFactory<M> factory() {
        return factory;
    }
}
