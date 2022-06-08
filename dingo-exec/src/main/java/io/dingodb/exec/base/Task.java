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

import io.dingodb.common.Location;
import io.dingodb.exec.operator.RootOperator;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Task {
    Id getId();

    Id getJobId();

    Location getLocation();

    RootOperator getRoot();

    Map<Id, Operator> getOperators();

    default String getHost() {
        return getLocation().getHost();
    }

    /**
     * Put an operator into the task. The implementation must make `operator.getTask().equals(this)` after the method is
     * called.
     *
     * @param operator the operator
     */
    void putOperator(@Nonnull Operator operator);

    void deleteOperator(@Nonnull Operator operator);

    List<Id> getRunList();

    void init();

    void run();

    @Nullable
    default Operator getOperator(Id id) {
        return getOperators().get(id);
    }

    byte[] serialize();

}
