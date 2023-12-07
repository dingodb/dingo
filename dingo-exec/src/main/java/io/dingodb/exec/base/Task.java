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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

public interface Task {
    CommonId getId();

    CommonId getJobId();

    CommonId getTxnId();

    Location getLocation();

    Operator getRoot();

    void markRoot(CommonId operatorId);

    Map<CommonId, Operator> getOperators();

    int getStatus();

    default String getHost() {
        return getLocation().getHost();
    }

    /**
     * Put an operator into the task. The implementation must make `operator.getTask().equals(this)` after the method is
     * called.
     *
     * @param operator the operator
     */
    void putOperator(@NonNull Operator operator);

    List<CommonId> getRunList();

    void init();

    /**
     * Run the task. This method should not be blocked.
     *
     * @param paras the paras for this run
     */
    void run(Object @Nullable [] paras);

    boolean cancel();

    default void destroy() {
        cancel(); // stop the task.
        getOperators().values().forEach(Operator::destroy);
    }

    default @Nullable Operator getOperator(CommonId id) {
        return getOperators().get(id);
    }

    DingoType getParasType();

    default void setParas(Object[] paras) {
        getOperators().values().forEach(o -> o.setParas(paras));
    }
}
