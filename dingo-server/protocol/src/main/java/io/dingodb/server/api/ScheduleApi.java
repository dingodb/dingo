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
import io.dingodb.net.api.annotation.ApiDeclaration;

public interface ScheduleApi {

    @ApiDeclaration
    void autoSplit(CommonId tableId, boolean auto);

    @ApiDeclaration
    void maxSize(CommonId tableId, long maxSize);

    @ApiDeclaration
    void maxCount(CommonId tableId, long maxCount);

    @ApiDeclaration
    void addReplica(CommonId tableId, CommonId partId, CommonId executorId);

    @ApiDeclaration
    void removeReplica(CommonId tableId, CommonId partId, CommonId executorId);

    @ApiDeclaration
    void transferLeader(CommonId tableId, CommonId partId, CommonId executorId);

    @ApiDeclaration
    void splitPart(CommonId tableId, CommonId partId);

    @ApiDeclaration
    void splitPart(CommonId tableId, CommonId partId, byte[] keys);

}
