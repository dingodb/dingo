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

package io.dingodb.exec.fun.time;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;

@Slf4j
public class FromUnixTimeFun extends RtFun {
    public static final String NAME = "from_unixtime";
    private static final long serialVersionUID = 9189202071207557600L;

    public FromUnixTimeFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        return new Timestamp(((Number) values[0]).longValue() * 1000L);
    }

    @Override
    public int typeCode() {
        return TypeCode.TIMESTAMP;
    }
}
