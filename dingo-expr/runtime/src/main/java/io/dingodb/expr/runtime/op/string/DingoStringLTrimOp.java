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
public class DingoStringLTrimOp extends RtStringConversionOp {
    private static final long serialVersionUID = -8557732786466948967L;

    /**
     * Create an DingoStringLTrimOp.
     * DingoStringTrimOp trim the leading blanks of a String.
     *
     * @param paras the parameters of the op
     */
    public DingoStringLTrimOp(RtExpr[] paras) {
        super(paras);
    }

    public static String trimLeft(final String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            int startIndex = 0;
            int endIndex = inputStr.length() - 1;
            for (; startIndex < inputStr.length(); startIndex++) {
                if (inputStr.charAt(startIndex) != ' ') {
                    break;
                }
            }

            return inputStr.substring(startIndex, endIndex + 1);
        }
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        String inputStr = (String) values[0];
        return trimLeft(inputStr);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringLTrimOp::new;
        }

        @Override
        public List<String> name() {
            return Collections.singletonList("ltrim");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringLTrimOp.class.getMethod("trimLeft", String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
