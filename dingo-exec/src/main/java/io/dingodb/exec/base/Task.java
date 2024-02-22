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
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

public interface Task {
    CommonId getId();

    CommonId getJobId();

    CommonId getTxnId();

    TransactionType getTransactionType();

    IsolationLevel getIsolationLevel();

    Location getLocation();

    Vertex getRoot();

    Context getContext();

    void markRoot(CommonId operatorId);

    Map<CommonId, Vertex> getVertexes();

    int getStatus();

    default String getHost() {
        return getLocation().getHost();
    }

    List<CommonId> getRunList();

    void putVertex(@NonNull Vertex vertex);

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
        getVertexes().values().forEach(Vertex::destroy);
    }

    default @Nullable Vertex getVertex(CommonId id) {
        return getVertexes().get(id);
    }

    DingoType getParasType();

    default void setParas(Object[] paras) {
        getVertexes().values().forEach(v -> v.setParas(paras));
    }
    default void setStartTs(long startTs) {
        getVertexes().values().forEach(v -> v.setStartTs(startTs));
    }
    void setTxnId(CommonId txnId);

    void setBathTask(boolean bathTask);

    void setContext(Context context);
}
