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

package io.dingodb.cli.handler;

import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.rebalance.DingoRebalancer;
import io.dingodb.helix.state.LeaderFollowerStateModelDefinition;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.model.builder.IdealStateBuilder;

import java.util.HashMap;
import java.util.Map;

public class SetupClusterHandler {
    public static void handler() {
        ServerConfiguration configuration = ServerConfiguration.instance();
        String zkServers = configuration.zkServers();
        String clusterName = configuration.clusterName();

        ZKHelixAdmin admin = new ZKHelixAdmin.Builder()
            .setZkAddress(zkServers)
            .build();
        admin.addCluster(clusterName);

        admin.addStateModelDef(
            clusterName,
            HelixPart.StateMode.LEADER_FOLLOWER.name,
            LeaderFollowerStateModelDefinition.generate()
        );

        Map<String, String> configMap = new HashMap<>();
        HelixConfigScope configScope =
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
        configMap.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, Boolean.toString(true));
        admin.setConfig(configScope, configMap);

        IdealStateBuilder coordinatorBuilder = new IdealStateBuilder(configuration.coordinatorName()) {
        };
        coordinatorBuilder
            .setStateModel(HelixPart.StateMode.LEADER_FOLLOWER.name)
            .setRebalancerMode(IdealState.RebalanceMode.USER_DEFINED)
            .setRebalancerClass(DingoRebalancer.class.getName())
            .setNumPartitions(configuration.coordinatorPartitions())
            .setNumReplica(configuration.coordinatorReplicas())
            .setMinActiveReplica(0);

        IdealState coordinatorIdealState = coordinatorBuilder.build();
        coordinatorIdealState.setInstanceGroupTag(configuration.coordinatorTag());
        admin.addResource(clusterName, coordinatorIdealState.getResourceName(), coordinatorIdealState);
        admin.rebalance(clusterName, configuration.coordinatorName(), configuration.coordinatorReplicas());
    }
}
