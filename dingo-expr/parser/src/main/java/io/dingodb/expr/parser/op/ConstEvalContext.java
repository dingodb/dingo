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

package io.dingodb.expr.parser.op;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.exception.NeverRunToHere;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConstEvalContext implements EvalContext {
    private static final long serialVersionUID = -4297360162936836019L;

    @Getter
    private final EvalEnv env;

    @Override
    public Object get(Object id) {
        throw new NeverRunToHere("Only constants are available in this context.");
    }

    @Override
    public void set(Object id, Object value) {
        throw new NeverRunToHere("Only constants are available in this context.");
    }
}
