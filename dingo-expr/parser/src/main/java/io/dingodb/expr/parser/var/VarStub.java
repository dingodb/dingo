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

package io.dingodb.expr.parser.var;

import io.dingodb.expr.parser.exception.ElementNotExists;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VarStub implements RtExpr {
    private static final long serialVersionUID = -8710109278953342879L;

    private final CompileContext ctx;

    @Override
    public Object eval(EvalContext etx) {
        throw new RuntimeException("Cannot eval a VarStub.");
    }

    @Override
    public int typeCode() {
        throw new RuntimeException("Cannot get typeCode of a VarStub.");
    }

    /**
     * Get the element by its index in the CompileContext contained in this VarStub, maybe a Var or VarStub.
     *
     * @param index the index of the element
     * @return the Var or VarStub
     * @throws ElementNotExists when an element of the specified index cannot be found
     */
    public RtExpr getElement(Object index) throws ElementNotExists {
        CompileContext child = ctx.getChild(index);
        if (child != null) {
            return Var.createVar(child);
        }
        throw new ElementNotExists(index, ctx);
    }
}
