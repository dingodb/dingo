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

package io.dingodb.exec.fun.mysql;

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;

public class InstrFun extends BinaryOp {
    private static final long serialVersionUID = -200033123590846940L;

    public static final InstrFun INSTANCE = new InstrFun();

    public static final String NAME = "instr";

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        String str = value0.toString();
        String point = value1.toString();
        return str.indexOf(point) + 1;
    }
}
