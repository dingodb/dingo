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

import io.dingodb.exec.fin.Fin;
import io.dingodb.meta.Location;

import javax.annotation.Nonnull;

public interface Output {
    Input getLink();

    void setLink(Input input);

    Operator getOperator();

    void setOperator(Operator operator);

    default boolean push(Object[] tuple) {
        Input link = getLink();
        return link.getOperator().push(link.getPin(), tuple);
    }

    default void fin(Fin fin) {
        Input link = getLink();
        link.getOperator().fin(link.getPin(), fin);
    }

    default Task getTask() {
        return getOperator().getTask();
    }

    default Id getTaskId() {
        return getTask().getId();
    }

    default Location getLocation() {
        return getTask().getLocation();
    }

    OutputHint getHint();

    void setHint(OutputHint hint);

    default void copyHint(@Nonnull Output output) {
        setHint(output.getHint());
    }

    default void init() {
        Input link = getLink();
        link.setOperator(getTask().getOperator(link.getOperatorId()));
    }

    default Location getTargetLocation() {
        OutputHint hint = getHint();
        return hint != null ? hint.getLocation() : null;
    }

    default boolean isToSumUp() {
        OutputHint hint = getHint();
        return hint != null && hint.isToSumUp();
    }
}
