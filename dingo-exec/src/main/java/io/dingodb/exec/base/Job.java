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

package io.dingodb.exec.base;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface Job {
    CommonId getJobId();

    Map<CommonId, Task> getTasks();

    @NonNull Task create(CommonId id, Location location, TransactionType transactionType, IsolationLevel isolationLevel);

    DingoType getParasType();

    default Task getTask(CommonId id) {
        return getTasks().get(id);
    }

    Task getRoot();

    void markRoot(CommonId taskId);

    default Task getByLocation(Location location) {
        List<Task> tasks = getTasks().values().stream()
            .filter(t -> t.getLocation().equals(location))
            .collect(Collectors.toList());
        assert tasks.size() <= 1 : "There should be at most one task at each location.";
        return tasks.size() == 1 ? tasks.get(0) : null;
    }

    default Task getOrCreate(Location location, IdGenerator idGenerator) {
        return getOrCreate(location, idGenerator, TransactionType.NONE, IsolationLevel.SnapshotIsolation);
    }

    default Task getOrCreate(Location location, IdGenerator idGenerator, TransactionType transactionType, IsolationLevel isolationLevel) {
        Task task = getByLocation(location);
        if (task == null) {
            task = create(idGenerator.getTaskId(), location, transactionType, isolationLevel);
        }
        return task;
    }

    default boolean isEmpty() {
        return getTasks().isEmpty();
    }

    default int getStatus() {
        return getRoot().getStatus();
    }

    default boolean cancel() {
        return getRoot().cancel();
    }

    default void setTxnId(CommonId txnId) {}
}
