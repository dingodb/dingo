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

package io.dingodb.helix.rebalance.impl;

import io.dingodb.helix.HelixAccessor;
import io.dingodb.helix.rebalance.ResourceAssigner;
import io.dingodb.helix.state.LeaderFollowerStateModelDefinition;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.helix.model.CurrentState.CurrentStateProperty.CURRENT_STATE;

public class LeaderFollowerResourceAssigner implements ResourceAssigner {

    private final HelixManager helixManager;
    private final HelixAccessor helixAccessor;

    public LeaderFollowerResourceAssigner(HelixManager helixManager) {
        this.helixManager = helixManager;
        this.helixAccessor = new HelixAccessor(
            helixManager.getConfigAccessor(),
            helixManager.getHelixDataAccessor(),
            helixManager.getClusterName()
        );
    }

    @Override
    public ResourceAssignment computeBestPossiblePartitionState(
        ResourceControllerDataProvider dataProvider,
        IdealState idealState,
        Resource resource,
        CurrentStateOutput currentStateOutput
    ) {
        String resourceName = resource.getResourceName();
        ResourceAssignment assignment = new ResourceAssignment(resourceName);

        // instance -> resource -> partition -> (fieldName -> fieldValue)
        // => zk(instance -> current states -> session -> resource -> partition -> map fields)
        Map<String, Map<String, Map<String, Map<String, String>>>> currentInstanceState = helixAccessor
            .instancesStates();

        Map<String, String> replicaMap;

        for (Partition partition : resource.getPartitions()) {
            List<String> availableInstances = idealState.getPreferenceList(partition.getPartitionName()).stream()
                .filter(currentInstanceState::containsKey)
                .collect(Collectors.toList());

            replicaMap = computeAssignment(
                resource,
                partition,
                idealState.getReplicaCount(currentInstanceState.size()),
                new HashMap<>(currentStateOutput.getCurrentStateMap(resourceName, partition)),
                new HashSet<>(availableInstances),
                currentInstanceState
            );

            assignment.addReplicaMap(partition, replicaMap);
            replicaMap.forEach((instance, state) -> currentInstanceState.get(instance)
                .compute(resourceName, (name, partMap) -> partMap == null ? new HashMap<>(8) : partMap)
                .compute(partition.getPartitionName(), (name, fields) -> fields == null ? new HashMap<>(8) : fields)
                .put(CURRENT_STATE.name(), state)
            );
        }

        return assignment;
    }

    /**
     * Compute assignment.
     *
     * @param currentPartitionState instance -> current state for current partition
     * @param currentInstanceState  instance -> resource -> partition -> (fieldName -> fieldValue) => zk(instance ->
     *                              current states -> session -> resource -> partition -> map fields)
     */
    private Map<String, String> computeAssignment(
        Resource resource,
        Partition partition,
        int replicaCount,
        Map<String, String> currentPartitionState,
        Set<String> availableInstances,
        Map<String, Map<String, Map<String, Map<String, String>>>> currentInstanceState
    ) {

        Map<String, String> result = new HashMap<>();

        if (currentInstanceState.isEmpty()) {
            return result;
        }

        String leader = null;
        Set<String> followerInstances = new HashSet<>();
        Set<String> offlineInstances = new HashSet<>();

        for (Map.Entry<String, String> instanceState : currentPartitionState.entrySet()) {
            switch (instanceState.getValue()) {
                case LeaderFollowerStateModelDefinition.LEADER:
                    if (availableInstances.contains(instanceState.getKey())) {
                        leader = instanceState.getKey();
                        availableInstances.remove(leader);
                    } else {
                        result.put(instanceState.getKey(), LeaderFollowerStateModelDefinition.OFFLINE);
                    }
                    break;
                case LeaderFollowerStateModelDefinition.FOLLOWER:
                    if (availableInstances.contains(instanceState.getKey())) {
                        followerInstances.add(instanceState.getKey());
                    } else {
                        result.put(instanceState.getKey(), LeaderFollowerStateModelDefinition.OFFLINE);
                    }
                    break;
                case LeaderFollowerStateModelDefinition.OFFLINE:
                    if (availableInstances.contains(instanceState.getKey())) {
                        offlineInstances.add(instanceState.getKey());
                    } else {
                        result.put(instanceState.getKey(), LeaderFollowerStateModelDefinition.DROPPED);
                    }
                    break;
                default:
                    break;
            }
        }

        if (leader == null) {
            leader = selectLeader(availableInstances, currentInstanceState, followerInstances, offlineInstances);
        }
        if (leader == null) {
            return result;
        }

        result.put(leader, LeaderFollowerStateModelDefinition.LEADER);
        replicaCount--;

        selectFollower(replicaCount, currentInstanceState, availableInstances, followerInstances, offlineInstances)
            .forEach(instance -> result.put(instance, LeaderFollowerStateModelDefinition.FOLLOWER));

        availableInstances.forEach(instance -> result.put(instance, LeaderFollowerStateModelDefinition.OFFLINE));

        return result;
    }

    private Set<String> selectFollower(
        int size,
        Map<String, Map<String, Map<String, Map<String, String>>>> currentInstanceState,
        Set<String> availableInstances,
        Set<String> followerInstances,
        Set<String> offlineInstances
    ) {
        if (size < followerInstances.size()) {
            followerInstances = sortInstanceByStateCount(
                size,
                currentInstanceState,
                availableInstances,
                offlineInstances
            );
        } else if (size > followerInstances.size()) {
            followerInstances.addAll(sortInstanceByStateCount(
                size - followerInstances.size(),
                currentInstanceState,
                availableInstances,
                offlineInstances)
            );
            if (size > followerInstances.size()) {
                followerInstances.addAll(sortInstanceByStateCount(
                    size - followerInstances.size(),
                    currentInstanceState,
                    availableInstances,
                    availableInstances
                ));
            }
        }
        return followerInstances;

    }

    private Set<String> sortInstanceByStateCount(
        int size,
        Map<String, Map<String, Map<String, Map<String, String>>>> currentInstanceState,
        Set<String> availableInstances,
        Set<String> candidateInstances
    ) {
        return candidateInstances.stream()
            .filter(availableInstances::contains)
            .map(instance -> new AbstractMap.SimpleEntry<>(instance, currentInstanceState.get(instance)
                .values()
                .stream()
                .flatMap(s -> s.values().stream())
                .map(map -> map.get(CURRENT_STATE.name()))
                .filter(state -> LeaderFollowerStateModelDefinition.LEADER.equals(state)
                    || LeaderFollowerStateModelDefinition.FOLLOWER.equals(state))
                .count()))
            .sorted(Map.Entry.comparingByValue())
            .limit(size)
            .map(AbstractMap.SimpleEntry::getKey)
            .peek(availableInstances::remove)
            .collect(Collectors.toSet());
    }

    private String selectLeader(
        Set<String> availableInstances,
        Map<String, Map<String, Map<String, Map<String, String>>>> currentInstanceState,
        Set<String> followerInstances,
        Set<String> offlineInstances
    ) {
        Optional<String> instance = minStateCountInstance(availableInstances, currentInstanceState, followerInstances);
        if (!instance.isPresent()) {
            instance = minStateCountInstance(availableInstances, currentInstanceState, offlineInstances);
        }
        if (!instance.isPresent()) {
            instance = minStateCountInstance(availableInstances, currentInstanceState, null);
        }

        return instance.orElse(null);
    }

    private Optional<String> minStateCountInstance(
        Set<String> availableInstances,
        Map<String, Map<String, Map<String, Map<String, String>>>> currentInstanceState,
        Set<String> candidateInstances
    ) {
        Optional<Set<String>> instancesStateOptional = Optional.ofNullable(candidateInstances);

        Optional<String> result = instancesStateOptional.orElse(availableInstances).stream()
            .filter(availableInstances::contains)
            .map(instance -> new AbstractMap.SimpleEntry<>(instance, currentInstanceState.get(instance)
                .values()
                .stream()
                .flatMap(s -> s.values().stream())
                .map(map -> map.get(CURRENT_STATE.name()))
                .filter(LeaderFollowerStateModelDefinition.LEADER::equals)
                .count()))
            .min(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);

        result.ifPresent(instance -> {
            instancesStateOptional.ifPresent(is -> is.remove(instance));
            availableInstances.remove(instance);
        });
        return result;
    }
}
