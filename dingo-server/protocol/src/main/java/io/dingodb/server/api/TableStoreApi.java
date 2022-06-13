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

package io.dingodb.server.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.store.Part;
import io.dingodb.net.api.annotation.ApiDeclaration;

public interface TableStoreApi {

    @ApiDeclaration
    void newTable(CommonId id);

    @ApiDeclaration
    void deleteTable(CommonId id);

    @ApiDeclaration
    void assignTablePart(Part part);

    @ApiDeclaration
    void reassignTablePart(Part part);

    @ApiDeclaration
    void removeTablePart(Part part);

    @ApiDeclaration
    void deleteTablePart(Part part);

    @ApiDeclaration
    void addTablePartReplica(CommonId table, CommonId part, Location replica);

    @ApiDeclaration
    void removeTablePartReplica(CommonId table, CommonId part, Location replica);

    @ApiDeclaration
    void transferLeader(CommonId table, CommonId part, Location leader);

}
