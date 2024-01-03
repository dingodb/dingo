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

package io.dingodb.store.proxy.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.config.VariableConfiguration;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.util.Optional;
import io.dingodb.store.proxy.Configuration;
import lombok.experimental.Delegate;

import static io.dingodb.store.proxy.common.Mapping.mapping;

public class AutoIncrementService {
    public static final AutoIncrementService INSTANCE = new AutoIncrementService();

    @Delegate
    private final io.dingodb.sdk.service.meta.AutoIncrementService autoIncrementService =
        new io.dingodb.sdk.service.meta.AutoIncrementService(Configuration.coordinators());

    private AutoIncrementService() {
    }

    public void resetAutoIncrement() {
        long count = Optional.ofNullable(DingoConfiguration.instance().getVariable())
            .map(VariableConfiguration::getAutoIncrementCacheCount)
            .ifAbsentSet(10000L)
            .filter(v -> v > 0)
            .orElseThrow("The config autoIncrementCacheCount must be a positive integer greater than 0.");

        Integer increment = Optional.ofNullable(DingoConfiguration.instance().getVariable())
            .map(VariableConfiguration::getAutoIncrementIncrement)
            .ifAbsentSet(
                () -> Integer.valueOf(ScopeVariables.globalVariables.getProperty("auto_increment_increment", "1")))
            .filter(v -> v > 0)
            .orElseThrow("The config autoIncrementIncrement must be a positive integer greater than 0.");

        Integer offset = Optional.ofNullable(DingoConfiguration.instance().getVariable())
            .map(VariableConfiguration::getAutoIncrementOffset)
            .ifAbsentSet(
                () -> Integer.valueOf(ScopeVariables.globalVariables.getProperty("auto_increment_offset", "1")))
            .filter(v -> v > 0)
            .orElseThrow("The config autoIncrementOffset must be a positive integer greater than 0.");

        autoIncrementService.reset(count, increment, offset);

    }

    public long getAutoIncrement(CommonId tableId) {
        return autoIncrementService.next(mapping(tableId));
    }

    public long getLastId(CommonId tableId) {
        return autoIncrementService.localCurrent(mapping(tableId));
    }

    public long getNextAutoIncrement(CommonId tableId) {return autoIncrementService.current(mapping(tableId)) + 1;}

    public void updateAutoIncrementId(CommonId tableId, long autoIncrementId) {
        autoIncrementService.update(mapping(tableId), autoIncrementId);
    }

}
