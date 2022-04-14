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

import io.dingodb.expr.runtime.var.RtVar;

public interface CompileContext {
    /**
     * Get the variable id of this context if it stands for a variable, or {@code null}.
     *
     * @return the id or {@code null}
     */
    default Object getId() {
        return null;
    }

    /**
     * Get the type code of this context. Type codes are mostly hashed from the type name of Java classes.
     *
     * @return the type code
     */
    int getTypeCode();

    /**
     * Get a specified child context of this context. A CompileContext may have child contexts.
     *
     * @param index then index of the child, can be a String (for Map index) or Integer (for Array index)
     * @return the child context
     */
    CompileContext getChild(Object index);

    default RtExpr createVar() {
        Object id = getId();
        return id != null ? new RtVar(id, getTypeCode()) : null;
    }
}
