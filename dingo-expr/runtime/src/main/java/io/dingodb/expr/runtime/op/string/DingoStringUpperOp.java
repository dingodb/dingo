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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class DingoStringUpperOp extends RtStringConversionOp {
    private static final long serialVersionUID = -4349256259193399655L;

    /**
     * Create an RtToUpperCaseOp. RtToUpperCaseOp converts a String to upper case by {@code String::toUpperCase}.
     *
     * @param paras the parameters of the op
     */
    public DingoStringUpperOp(RtExpr[] paras) {
        super(paras);
    }

    public static String toUpCase(final String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.toUpperCase();
        }
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        return toUpCase((String) values[0]);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringUpperOp::new;
        }

        @Override
        public List<String> name() {
            return Collections.singletonList("ucase");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringUpperOp.class.getMethod("toUpCase", String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
