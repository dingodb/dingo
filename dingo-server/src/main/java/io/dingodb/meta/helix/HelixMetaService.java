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

package io.dingodb.meta.helix;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.coordinator.TableIdealStateBuilder;
import io.dingodb.helix.HelixAccessor;
import io.dingodb.helix.part.HelixPart;
import io.dingodb.meta.MetaService;
import io.dingodb.net.Location;
import io.dingodb.server.Constants;
import io.dingodb.server.ServerConfiguration;
import io.dingodb.server.ServerError;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static io.dingodb.common.table.TableDefinition.fromJson;
import static io.dingodb.helix.state.LeaderFollowerStateModelDefinition.LEADER;
import static io.dingodb.server.Constants.PATHS;
import static io.dingodb.server.Constants.TABLE_DEFINITION_KEY;
import static io.dingodb.server.ServerError.CREATE_META_SUCCESS_WAIT_LEADER_FAILED;
import static io.dingodb.server.ServerError.TO_JSON_ERROR;

@Slf4j
public class HelixMetaService implements MetaService {

    protected static final HelixMetaService INSTANCE = new HelixMetaService();
    protected HelixPart helixPart;
    protected HelixAdmin helixAdmin;
    protected HelixAccessor helixAccessor;
    protected ServerConfiguration configuration;
    protected String clusterName;
    protected Map<String, Object> props;
    protected boolean helixActive = false;

    protected HelixMetaService() {
    }

    public static HelixMetaService instance() {
        return INSTANCE;
    }

    public void init(HelixPart helixPart) {
        this.helixPart = helixPart;
        this.helixAdmin = helixPart.helixManager().getClusterManagmentTool();
        this.helixAccessor = helixPart.helixAccessor();
        this.clusterName = helixPart.clusterName();
        configuration = helixPart.configuration();
        helixActive = true;
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {
        this.props = props;
    }

    protected void checkAndWaitHelixActive() {
        int times = 5;
        while (times-- > 0) {
            try {
                if (helixActive) {
                    return;
                }
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                log.error("Wait helix active interrupted");
                ServerError.CHECK_AND_WAIT_HELIX_ACTIVE.throwError();
            }
        }
        ServerError.CHECK_AND_WAIT_HELIX_ACTIVE.throwError();
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean dropTable(@Nonnull String name) {
        checkAndWaitHelixActive();
        if (!isCoordinatorLeader()) {
            throw new RuntimeException("Only coordinator leader drop table.");
        }
        deleteResource(name, name);
        return true;
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        checkAndWaitHelixActive();
        List<String> resources = helixAdmin.getResourcesInCluster(clusterName);
        Map<String, TableDefinition> tableDefinitions = new HashMap<>();
        for (String name : resources) {
            if (name.equals(configuration.coordinatorName())) {
                continue;
            }
            ResourceConfig resource = helixAccessor.getResourceConfig(clusterName, name);
            try {
                tableDefinitions.put(name, fromJson(resource.getSimpleConfig(TABLE_DEFINITION_KEY)));
            } catch (Exception e) {
                tableDefinitions.put(name, null);
            }
        }
        return tableDefinitions;
    }

    @Override
    public Map<Object, Location> getPartLocations(String name) {
        checkAndWaitHelixActive();
        Map<Object, Location> result = new HashMap<>();
        ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, name);
        for (String partitionName : externalView.getPartitionSet()) {
            String[] ss = partitionName.split(Constants.SEP);
            int partitionId = Integer.parseInt(ss[ss.length - 1]);
            externalView.getStateMap(partitionName).entrySet().stream()
                .filter(e -> LEADER.equals(e.getValue()))
                .findFirst()
                .ifPresent(instance -> result.put(partitionId, getInstanceLocation(instance.getKey())));
        }
        return result;
    }

    private Location getInstanceLocation(String instance) {
        InstanceConfig instanceConfig = helixAccessor.getInstanceConfig(clusterName, instance);
        List<String> paths = instanceConfig.getRecord().getListField(PATHS);
        return new Location(instanceConfig.getHostName(), Integer.parseInt(instanceConfig.getPort()), paths.get(0));
    }

    @Override
    public Location currentLocation() {
        return new Location(configuration.instanceHost(), configuration.port(), configuration.dataDir());
    }

    @Override
    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        if (!isCoordinatorLeader()) {
            throw new RuntimeException("Only coordinator leader create table.");
        }
        createResource(
            tableName,
            configuration.defaultReplicas(),
            configuration.defaultPartitions(),
            configuration.tableTag(),
            tableDefinition
        );
    }

    private void createResource(
        String tableName,
        int numReplica,
        int numPartitions,
        String tag,
        TableDefinition tableDefinition
    ) {
        checkAndWaitHelixActive();
        // Build the table ideal state.
        IdealState tableIdealState = new TableIdealStateBuilder(tableName).setNumReplica(numReplica)
            .setNumPartitions(numPartitions)
            .setTag(tag)
            .build();

        // Add table tag to every instance.
        List<String> instances = helixAdmin.getInstancesInCluster(clusterName);
        for (String instance : instances) {
            InstanceConfig instanceConfig = helixAccessor.getInstanceConfig(clusterName, instance);
            if (instanceConfig.containsTag(configuration.coordinatorTag())) {
                continue;
            }
            instanceConfig.addTag(tag);
            helixAccessor.setInstanceConfig(clusterName, instance, instanceConfig);
        }

        // Add table resource, put table definition to resource config.
        helixAdmin.addResource(clusterName, tableName, tableIdealState);
        ResourceConfig resourceConfig = helixAccessor.getResourceConfig(clusterName, tableName);
        try {
            if (resourceConfig == null) {
                resourceConfig = new ResourceConfig(tableName);
            }
            resourceConfig.putSimpleConfig(TABLE_DEFINITION_KEY, tableDefinition.toJson());
        } catch (JsonProcessingException e) {
            TO_JSON_ERROR.throwError("create table", e.getMessage());
        }
        helixAccessor.setResourceConfig(clusterName, tableName, resourceConfig);

        // Rebalance.
        helixAdmin.rebalance(clusterName, tableName, numReplica);

        int times = 5;
        while (times-- > 0) {
            Optional<Boolean> leaderActive = Optional
                .ofNullable(helixAdmin.getResourceExternalView(clusterName, tableName))
                .map(ev -> {
                    for (String partitionName : ev.getPartitionSet()) {
                        if (ev.getStateMap(partitionName).values().stream().noneMatch(LEADER::equals)) {
                            return false;
                        }
                    }
                    return true;
                });
            if (leaderActive.isPresent() && leaderActive.get()) {
                return;
            } else {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException e) {
                    log.error("Wait table leader interrupt, table name [{}]", tableName);
                }
            }
            CREATE_META_SUCCESS_WAIT_LEADER_FAILED.throwError(tableName);
        }
    }

    private boolean isCoordinatorLeader() {
        checkAndWaitHelixActive();
        //String coordinatorName = configuration.coordinatorName();
        //ExternalView coordinatorView = helixAdmin.getResourceExternalView(clusterName, coordinatorName);
        //Map<String, String> stateMap = coordinatorView.getStateMap(coordinatorName + "_" + 0);
        //return stateMap != null && LEADER.equals(stateMap.get(helixPart.name()));
        // todo sqlline fix
        return true;
    }

    private void deleteResource(
        String tableName,
        String tag
    ) {
        checkAndWaitHelixActive();

        // Drop table resource.
        helixAdmin.dropResource(clusterName, tableName);

        // Remove table resource tag for every instance.
        List<String> instances = helixAdmin.getInstancesInCluster(clusterName);
        for (String instance : instances) {
            InstanceConfig instanceConfig = helixAccessor.getInstanceConfig(clusterName, instance);
            instanceConfig.removeTag(tag);
            helixAccessor.setInstanceConfig(clusterName, instance, instanceConfig);
        }
    }

}
