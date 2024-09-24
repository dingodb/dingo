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

package io.dingodb.server.executor.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.ReorgBackFillTask;
import io.dingodb.server.executor.ddl.BackFillResult;

import java.util.ArrayList;
import java.util.List;

public interface BackFiller {
    boolean preWritePrimary(ReorgBackFillTask task);
    BackFillResult backFillDataInTxn(ReorgBackFillTask task, boolean withCheck);
    BackFillResult backFillDataInTxnWithCheck(ReorgBackFillTask task, boolean withCheck);

    boolean commitPrimary();
    boolean commitSecond();

    default void close() {}

    default long getScanCount(){ return 0; }
    default long getAddCount(){ return 0; }

    default long getCommitCount() { return  0; }

    default long getConflictCount() { return 0; }

    default List<CommonId> getDoneRegion() { return new ArrayList<>(); }

}
