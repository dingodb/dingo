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

import io.dingodb.helix.part.HelixPart;
import org.apache.helix.model.StateModelDefinition;

public final class LeaderFollowerStateModelDefinition {

    public static final String OFFLINE = "OFFLINE";
    public static final String LEADER = "LEADER";
    public static final String FOLLOWER = "FOLLOWER";
    public static final String DROPPED = "DROPPED";
    public static final String ERROR = "ERROR";

    private LeaderFollowerStateModelDefinition() {
    }

    public static StateModelDefinition generate() {
        StateModelDefinition.Builder builder =
            new StateModelDefinition.Builder(HelixPart.StateMode.LEADER_FOLLOWER.name);

        builder
            .addState(OFFLINE, Integer.MAX_VALUE - 1)
            .addState(LEADER, Integer.MAX_VALUE - 2)
            .addState(FOLLOWER, Integer.MAX_VALUE - 1)
            .addState(DROPPED, Integer.MAX_VALUE - 1);

        builder.initialState(OFFLINE);

        builder.addTransition(OFFLINE, LEADER, Integer.MAX_VALUE - 2);
        builder.addTransition(OFFLINE, FOLLOWER, Integer.MAX_VALUE - 1);
        builder.addTransition(OFFLINE, DROPPED, Integer.MAX_VALUE - 1);
        builder.addTransition(LEADER, FOLLOWER, Integer.MAX_VALUE - 1);
        builder.addTransition(LEADER, OFFLINE, Integer.MAX_VALUE - 1);
        builder.addTransition(FOLLOWER, LEADER, Integer.MAX_VALUE - 1);
        builder.addTransition(FOLLOWER, OFFLINE, Integer.MAX_VALUE - 1);

        builder.dynamicUpperBound(LEADER, "1");
        builder.dynamicUpperBound(FOLLOWER, "R");

        return builder.build();
    }

}
