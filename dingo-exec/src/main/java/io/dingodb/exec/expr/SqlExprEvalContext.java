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

package io.dingodb.exec.expr;

import io.dingodb.expr.runtime.EvalContext;
import lombok.Setter;

import java.util.Map;

public class SqlExprEvalContext implements EvalContext {
    private static final long serialVersionUID = 9182182810857271788L;

    @Setter
    private Map<String, Object> paras = null;
    @Setter
    private Object[] tuple = null;

    public SqlExprEvalContext() {
    }

    public SqlExprEvalContext(Object[] tuple) {
        this.tuple = tuple;
    }

    @Override
    public Object get(Object id) {
        if (id instanceof Integer) {
            return tuple[(int) id];
        } else if (id instanceof String) {
            assert paras != null : "Parameters are not available in this context.";
            return paras.get(id);
        }
        return null;
    }

    @Override
    public void set(Object id, Object value) {
        if (id instanceof Integer) {
            tuple[(int) id] = value;
        } else if (id instanceof String) {
            paras.put((String) id, value);
        }
    }
}
