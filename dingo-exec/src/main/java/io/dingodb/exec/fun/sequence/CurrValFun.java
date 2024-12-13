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

package io.dingodb.exec.fun.sequence;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.meta.SequenceService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CurrValFun extends UnaryOp {
    private static final long serialVersionUID = 6470674959571877306L;
    public static final String NAME = "CURRVAL";
    public static final CurrValFun INSTANCE = new CurrValFun();


    @Override
    protected Object evalNonNullValue(@NonNull Object value, ExprConfig config) {
        String sequence = (String) value;
        SequenceService sequenceService = SequenceService.getDefault();
        return sequenceService.currVal(sequence);
    }

    @Override
    public Type getType() {
        return Types.LONG;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
