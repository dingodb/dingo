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

import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Content;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Operator {

    /**
     * Push a new tuple to the operator. Need to be synchronized for there may be multiple thread call on the same
     * operator.
     *
     * @param content  the input pin no and distribution
     * @param tuple the tuple pushed in
     * @return `true` means another push needed, `false` means the task is canceled or finished
     */
    boolean push(Content content, @Nullable Object[] tuple, Vertex vertex);

    void fin(int pin, @Nullable Fin fin, Vertex vertex);

    void setParas(Object[] paras);

}
