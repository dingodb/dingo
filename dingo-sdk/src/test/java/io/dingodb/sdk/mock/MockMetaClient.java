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

package io.dingodb.sdk.mock;

import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.MetaClient;
import io.dingodb.sdk.utils.MetaServiceUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;


@Slf4j
public class MockMetaClient extends MetaClient {
    public MockMetaClient(String coordinatorExchangeSvrList) {
        super(coordinatorExchangeSvrList);
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        log.info("mock to create table:{}, definition:{}", tableName, tableDefinition);
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        log.info("mock to drop table:{}", tableName);
        return true;
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String name) {
        return MetaServiceUtils.getSimpleTableDefinition(name);
    }
}
