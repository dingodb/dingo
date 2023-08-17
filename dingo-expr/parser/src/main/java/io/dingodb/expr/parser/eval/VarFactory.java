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

package io.dingodb.expr.parser.eval;

import io.dingodb.expr.parser.exception.ElementNotExists;
import io.dingodb.expr.parser.exception.NeverRunToHere;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.var.IndexedVar;
import io.dingodb.expr.runtime.eval.var.NamedVar;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class VarFactory {
    private VarFactory() {
    }

    public static @NonNull Eval of(Object index, @NonNull CompileContext context) {
        CompileContext child = context.getChild(index);
        if (child != null) {
            Object id = child.getId();
            if (id != null) {
                if (id instanceof Integer) {
                    return new IndexedVar((Integer) id, child.getTypeCode());
                } else if (id instanceof String) {
                    return new NamedVar((String) id, child.getTypeCode());
                }
                throw new NeverRunToHere("Illegal var index type \"" + id.getClass().getCanonicalName() + "\".");
            }
            return new EvalStub(child);
        }
        throw new ElementNotExists(index.toString(), context);
    }
}
