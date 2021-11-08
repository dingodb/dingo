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

package io.dingodb.helix;

import lombok.experimental.Delegate;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.apache.helix.model.LiveInstance.LiveInstanceProperty.SESSION_ID;

public class HelixAccessor {

    @Delegate
    private ConfigAccessor configAccessor;

    @Delegate
    private PropertyKey.Builder propertyKeyBuilder;

    private HelixDataAccessor dataAccessor;

    public HelixAccessor(ConfigAccessor configAccessor, HelixDataAccessor dataAccessor, String clusterName) {
        this.configAccessor = configAccessor;
        this.dataAccessor = dataAccessor;
        propertyKeyBuilder = new PropertyKey.Builder(clusterName);
    }

    /**
     * Returns the state of all instance in the cluster.
     *
     * @return instance -> resource -> partition -> (fieldName -> fieldValue) => zk(instance -> current states ->
     *     session -> resource -> partition -> map fields)
     */
    public Map<String, Map<String, Map<String, Map<String, String>>>> instancesStates() {
        Map<String, LiveInstance> liveInstances = dataAccessor.getChildValuesMap(liveInstances(), true);
        return dataAccessor.getChildNames(instances()).stream()
            .filter(liveInstances::containsKey)
            .collect(toMap(i -> i, instance -> instanceStates(instance, getSessionId(liveInstances.get(instance)))));
    }

    public String getSessionId(String instance) {
        return getSessionId(dataAccessor.getProperty(liveInstance(instance)));
    }

    public String getSessionId(LiveInstance liveInstance) {
        return liveInstance.getRecord().getSimpleField(SESSION_ID.name());
    }

    public Map<String, Map<String, Map<String, String>>> instanceStates(String instance, String sessionId) {
        return dataAccessor.<CurrentState>getChildValuesMap(currentStates(instance, sessionId), true).entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                e -> mapFields(e.getValue())
            ));
    }

    public void setSimpleField(PropertyKey propertyKey, String fieldName, String fieldValue) {
        HelixProperty property = dataAccessor.getProperty(propertyKey);
        property.getRecord().setSimpleField(fieldName, fieldValue);
        dataAccessor.setProperty(propertyKey, property);
    }

    public String getSimpleField(PropertyKey propertyKey, String fieldName) {
        return dataAccessor.getProperty(propertyKey).getRecord().getSimpleField(fieldName);
    }

    public Map<String, Map<String, String>> mapFields(PropertyKey propertyKey) {
        return mapFields(dataAccessor.getProperty(propertyKey).getRecord());
    }

    public Map<String, Map<String, String>> mapFields(HelixProperty property) {
        return mapFields(property.getRecord());
    }

    public Map<String, Map<String, String>> mapFields(ZNRecord record) {
        return record.getMapFields().entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> new HashMap<>(e.getValue())
        ));
    }

    public Map<String, List<String>> listFields(PropertyKey propertyKey) {
        return listFields(dataAccessor.getProperty(propertyKey).getRecord());
    }

    public Map<String, List<String>> listFields(HelixProperty property) {
        return listFields(property.getRecord());
    }

    public Map<String, List<String>> listFields(ZNRecord record) {
        return record.getListFields().entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> new ArrayList<>(e.getValue())
        ));
    }

    public void addToList(PropertyKey key, String name, String value) {
        HelixProperty property = dataAccessor.getProperty(key);
        List<String> oldValues = property.getRecord().getListField(name);
        if (oldValues == null) {
            oldValues = Collections.singletonList(value);
        } else {
            if (oldValues.contains(value)) {
                return;
            }
            (oldValues = new ArrayList<>(oldValues)).add(value);
        }
        property.getRecord().setListField(name, oldValues);
        dataAccessor.setProperty(key, property);
    }

    public void addAllToList(PropertyKey key, String name, List<String> values) {
        HelixProperty property = dataAccessor.getProperty(key);
        List<String> oldValues = property.getRecord().getListField(name);
        if (oldValues != null) {
            values = values.stream().filter(v -> !oldValues.contains(v)).collect(Collectors.toList());
            if (values.isEmpty()) {
                return;
            }
            values.addAll(oldValues);
        }
        property.getRecord().setListField(name, values);
        dataAccessor.setProperty(key, property);
    }

}
