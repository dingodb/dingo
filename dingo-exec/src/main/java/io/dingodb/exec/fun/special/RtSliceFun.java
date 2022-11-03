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

package io.dingodb.exec.fun.special;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class RtSliceFun extends RtFun {
    public static final String NAME = "$SLICE";
    private static final long serialVersionUID = 385165019484223289L;

    public RtSliceFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.OBJECT;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object fun(Object @NonNull [] values) {
        List<Object[]> list = (List<Object[]>) values[0];
        return list.stream()
            .map(t -> t[0])
            .collect(Collectors.toList());
    }
}
