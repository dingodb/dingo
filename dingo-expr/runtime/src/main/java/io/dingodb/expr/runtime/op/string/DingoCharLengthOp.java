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

package io.dingodb.expr.runtime.op.string;

import com.google.auto.service.AutoService;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoCharLengthOp extends RtStringConversionOp {
    private static final long serialVersionUID = 5454356467741754567L;

    /**
     * Create an DingoStringConcatOp. concat all input args.
     *
     * @param paras the parameters of the op
     */
    public DingoCharLengthOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values[0] == null) {
            return 0;
        }
        return charLength((String) values[0]);
    }

    public static int charLength(String str) {
        return str.length();
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoCharLengthOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("char_length");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoCharLengthOp.class.getMethod("charLength", String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
