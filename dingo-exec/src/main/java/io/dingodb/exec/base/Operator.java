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
import io.dingodb.expr.runtime.CompileContext;

import java.util.Collection;
import java.util.Map;

import static io.dingodb.common.util.Utils.sole;

public interface Operator {
    Id getId();

    void setId(Id id);

    Task getTask();

    void setTask(Task task);

    default void init() {
        getOutputs().forEach(o -> {
            o.setOperator(this);
            o.init();
        });
    }

    /**
     * Push a new tuple to the operator. Need to be synchronized for there may be multiple thread call on the same
     * operator.
     *
     * @param pin   the input pin no
     * @param tuple the tuple pushed in
     * @return `true` means another push needed, `false` means the task is canceled or finished
     */
    boolean push(int pin, Object[] tuple);

    void fin(int pin, Fin fin);

    void reset();

    void setParas(Map<String, Object> paras);

    default Input getInput(int pin) {
        Input input = new Input(getId(), pin);
        input.setOperator(this);
        return input;
    }

    Collection<Output> getOutputs();

    /**
     * Get the only output of the operator. Exception is thrown if there are more than one outputs.
     *
     * @return the only output
     */
    default Output getSoleOutput() {
        return sole(getOutputs());
    }

    default CompileContext getParasCompileContext() {
        return getTask().getParasCompileContext();
    }
}
