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

package io.dingodb.expr.runtime;

public class TupleEvalContext implements EvalContext {
    private static final long serialVersionUID = -1735756800219588237L;

    private final Object[] tuple;

    /**
     * Create a TupleEvalContext with a tuple.
     *
     * @param tuple the tuple
     */
    public TupleEvalContext(Object[] tuple) {
        this.tuple = tuple;
    }

    @Override
    public String toString() {
        if (tuple.length == 0) {
            return "(empty)";
        }
        if (tuple.length == 1) {
            return tuple[0].toString();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tuple.length; i++) {
            sb.append(String.format("%03d", i));
            sb.append(": ");
            sb.append(tuple[i]);
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public Object get(Object id) {
        return tuple[(int) id];
    }

    @Override
    public void set(Object id, Object value) {
        tuple[(int) id] = value;
    }
}
