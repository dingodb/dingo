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

import io.dingodb.server.Constants;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SetupExecutorHandler {

    public static void handler() throws Exception {
        ServerConfiguration configuration = ServerConfiguration.instance();
        String zkServers = configuration.zkServers();
        String clusterName = configuration.clusterName();
        String instanceName = configuration.instanceHost() + "_" + configuration.executorPort();

        ZKHelixAdmin helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(zkServers).build();

        InstanceConfig instanceConfig = new InstanceConfig(instanceName);

        // Add all resource tag to executor config.
        List<IdealState> resourceConfigs = helixAdmin
            .getResourcesInCluster(clusterName).stream()
            .filter(resource -> !resource.equals(configuration.coordinatorName()))
            .map(resource -> helixAdmin.getResourceIdealState(clusterName, resource))
            .collect(Collectors.toList());

        resourceConfigs.forEach(config -> instanceConfig.addTag(config.getInstanceGroupTag()));

        // Add local path to executor config.
        List<String> paths = Arrays.asList(configuration.dataDir().split(","));
        paths.stream().map(File::new).filter(f -> !f.exists()).forEach(File::mkdirs);
        instanceConfig.getRecord().setListField(Constants.PATHS, paths);
        instanceConfig.setHostName(configuration.instanceHost());
        instanceConfig.setPort(String.valueOf(configuration.executorPort()));
        instanceConfig.setInstanceEnabled(true);
        instanceConfig.addTag(configuration.tableTag());

        helixAdmin.addInstance(clusterName, instanceConfig);

    }

}
