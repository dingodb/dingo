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

package io.dingodb.common.operation.executive;

import io.dingodb.common.operation.Executive;
import io.dingodb.common.operation.compute.number.ComputeDouble;
import io.dingodb.common.operation.compute.number.ComputeFloat;
import io.dingodb.common.operation.compute.number.ComputeInteger;
import io.dingodb.common.operation.compute.number.ComputeLong;
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.compute.number.ComputeZero;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.store.KeyValue;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.Iterator;
import java.util.List;

public abstract class NumberExecutive<D extends OperationContext, T extends Iterator<KeyValue>, R>
    implements Executive<D, T, R> {

    Globals globals = JsePlatform.standardGlobals();

    private void addLuajFunction() {
        String filter =
            "function test(o) \r\n"
                + "return o > 35 \r\n"
                + "end";
        globals.load(filter).call();
    }

    public ComputeNumber convertType(Object value) {
        if (value instanceof Integer) {
            return new ComputeInteger((Integer) value);
        } else if (value instanceof Long) {
            return new ComputeLong((Long) value);
        } else if (value instanceof Double) {
            return new ComputeDouble((Double) value);
        } else if (value instanceof Float) {
            return new ComputeFloat((Float) value);
        } else {
            return new ComputeZero();
        }
    }

    public LuaValue convertToLuaValue(Object value) {
        if (value instanceof Integer || value instanceof Long) {
            return LuaValue.valueOf((int) value);
        } else if (value instanceof Double || value instanceof Float) {
            return LuaValue.valueOf((double) value);
        } else {
            return LuaValue.valueOf(value.toString());
        }
    }
}
