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

import io.dingodb.helix.HelixAccessor;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.manager.zk.GenericZkHelixApiBuilder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

import java.lang.reflect.Field;

public class ResourceTagHandler {

    public static void handler(String name, String tag) throws NoSuchFieldException {
        ServerConfiguration configuration = ServerConfiguration.instance();
        String zkServers = configuration.zkServers();
        String clusterName = configuration.clusterName();

        ZKHelixAdmin helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(zkServers).build();

        IdealState idealState = helixAdmin.getResourceIdealState(clusterName, name);
        idealState.setInstanceGroupTag(tag);
        helixAdmin.setResourceIdealState(clusterName, name, idealState);

        HelixAccessor helixAccessor = new HelixAccessor(null, new ZKHelixDataAccessor(clusterName,
            new ZkBaseDataAccessor.Builder<ZNRecord>().setZkAddress(zkServers).build()), clusterName);

        helixAccessor.setSimpleField(
            helixAccessor.externalView(name),
            ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
            tag
        );
        RebalanceHandler.handler(name, Integer.valueOf(idealState.getReplicas()));
    }

}
