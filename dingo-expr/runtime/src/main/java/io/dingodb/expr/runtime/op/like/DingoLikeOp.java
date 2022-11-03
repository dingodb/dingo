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

package io.dingodb.expr.runtime.op.like;

import com.google.auto.service.AutoService;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.utils.LikeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

@Slf4j
public class DingoLikeOp extends RtFun {
    private final Pattern pattern;

    public DingoLikeOp() {
        super(null);
        pattern = null;
    }

    public DingoLikeOp(@Nonnull RtExpr[] paras) {
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

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoLikeOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("like");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoLikeOp.class.getMethod("like", String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
