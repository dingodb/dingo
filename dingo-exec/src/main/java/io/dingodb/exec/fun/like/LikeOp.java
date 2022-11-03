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

package io.dingodb.exec.fun.like;

import io.dingodb.exec.utils.LikeUtils;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;
import javax.annotation.Nonnull;

@Slf4j
public class LikeOp extends RtFun {
    public static final String NAME = "like";
    private static final long serialVersionUID = 2318363563124744391L;
    private final Pattern pattern;

    public LikeOp() {
        super(null);
        pattern = null;
    }

    public LikeOp(@Nonnull RtExpr[] paras) {
        super(paras);

        String patternStr = "";
        if (paras[1] != null) {
            patternStr = String.valueOf(((RtConst) paras[1]).getValue());
        }
        pattern = LikeUtils.getPattern(patternStr, false);
    }

    public Boolean like(final String value) {
        return pattern.matcher(value).matches();
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values[0] == null) {
            return null;
        }
        return like(String.valueOf(values[0]));
    }

    @Override
    public int typeCode() {
        return TypeCode.BOOL;
    }
}
