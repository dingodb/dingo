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

package io.dingodb.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.table.TableDefinition;

import java.util.Map;

public interface MetaService extends SchemaService, TableService {

    static MetaService root() {
        return MetaServiceProvider.getDefault().root();
    }

    String ROOT_NAME = "DINGO_ROOT";
    String DINGO_NAME = "DINGO";
    String META_NAME = "META";

    /**
     * Returns this meta service id.
     *
     * @return this id
     */
    CommonId id();

    /**
     * Returns this meta service name.
     *
     * @return this name
     */
    String name();

    /**
     * Whether current meta service is root meta service.
     * @return true if current meta service is root
     */
    default boolean isRoot() {
        return id().equals(root().id());
    }

    /**
     * Create sub meta service.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name sub meta service name
     */
    void createSubMetaService(String name);

    /**
     * Get all sub meta services.
     *
     * @return all sub meta services
     */
    Map<String, MetaService> getSubMetaServices();

    /**
     * Get sub meta service by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name meta service name
     * @return sub meta service
     */
    MetaService getSubMetaService(String name);

    /**
     * Drop sub meta service by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name meta service name
     * @return true if success
     */
    boolean dropSubMetaService(String name);

    default Map<CommonId, Long> getAllTableCommitCount() {
        throw new UnsupportedOperationException();
    }

    default Map<CommonId, Long> getAllTableCommitIncrement() {
        throw new UnsupportedOperationException();
    }

    default void addDistribution(String tableName, PartitionDetailDefinition detail) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns current process location.
     *
     * @return current process location
     */
    default Location currentLocation() {
        return DingoConfiguration.location();
    }
}
