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

package io.dingodb.coordinator;

import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.rebalance.DingoRebalancer;
import io.dingodb.server.Constants;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.IdealStateBuilder;

public class TableIdealStateBuilder {

    private IdealStateBuilder idealStateBuilder;

    private String tag;

    public TableIdealStateBuilder(String resourceName) {
        idealStateBuilder = new IdealStateBuilder(resourceName) {
        };
        idealStateBuilder.setStateModel(HelixPart.StateMode.LEADER_FOLLOWER.name);
        idealStateBuilder.setRebalancerClass(DingoRebalancer.class.getName());
        idealStateBuilder.setRebalancerMode(IdealState.RebalanceMode.USER_DEFINED);
        idealStateBuilder.setResourceType(Constants.TABLE_KEY);
        idealStateBuilder.setMinActiveReplica(1);
    }

    public TableIdealStateBuilder setNumReplica(int numReplica) {
        idealStateBuilder.setNumReplica(numReplica);
        return this;
    }

    public TableIdealStateBuilder setNumPartitions(int numPartitions) {
        idealStateBuilder.setNumPartitions(numPartitions);
        return this;
    }

    public TableIdealStateBuilder setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public IdealState build() {
        IdealState idealState = this.idealStateBuilder.build();
        idealState.setInstanceGroupTag(tag);
        return idealState;
    }
}
