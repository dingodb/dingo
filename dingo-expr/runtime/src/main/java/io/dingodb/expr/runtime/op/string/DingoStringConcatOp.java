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
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@Slf4j
public class DingoStringConcatOp extends RtStringConversionOp {
    private static final long serialVersionUID = 5454356467741754567L;

    /**
     * Create an DingoStringConcatOp. concat all input args.
     *
     * @param paras the parameters of the op
     */
    public DingoStringConcatOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values.length != 2) {
            throw new IllegalArgumentException("concat only accept 2 args, current input args: " + values.length);
        }
        String inputStr1 = (values[0] == null ? "" : values[0].toString());
        String inputStr2 = (values[1] == null ? "" : values[1].toString());
        return concat(inputStr1, inputStr2);
    }

    public static String concat(String str1, String str2) {
        return str1 + str2;
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringConcatOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("concat");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringConcatOp.class.getMethod("concat", String.class, String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }

}
