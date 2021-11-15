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

import io.dingodb.net.Location;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public interface Job {
    List<Task> getTasks();

    @Nonnull
    Task getOrCreate(Location location);

    default Task getRootTask() {
        List<Task> tasks = getTasks().stream()
            .filter(t -> t.getRoot() != null)
            .collect(Collectors.toList());
        assert tasks.size() == 1 : "There must be only one root task in the job.";
        return tasks.get(0);
    }

    default boolean isEmpty() {
        return getTasks().isEmpty();
    }
}
