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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nullable;

@RequiredArgsConstructor
@Getter
public class RtConst implements RtExpr {
    public static final RtConst TAU = new RtConst(6.283185307179586476925);
    public static final RtConst E = new RtConst(2.7182818284590452354);
    public static final RtConst TRUE = new RtConst(true);
    public static final RtConst FALSE = new RtConst(false);

    private static final long serialVersionUID = -5457707032677852803L;

    private final Object value;

    @Nullable
    @Override
    public Object eval(EvalContext etx) {
        return value;
    }

    @Override
    public int typeCode() {
        return TypeCodes.getTypeCode(value);
    }
}
