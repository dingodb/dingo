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

package io.dingodb.server.coordinator.meta.adaptor;

import io.dingodb.common.CommonId;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.store.api.StoreInstance;

import java.util.Collection;
import java.util.List;

public interface Adaptor<M> extends StoreInstance {

    CommonId id();

    Class<M> adaptFor();

    void reload();

    CommonId metaId();

    CommonId save(M meta);

    M get(CommonId id);

    void delete(CommonId id);

    List<M> getByDomain(int domain);

    Collection<M> getAll();

    String[] arrayValues(M meta);

    TableDefinition getDefinition();

}
