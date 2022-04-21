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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class DingoStringLocateOp extends RtStringConversionOp {
    private static final long serialVersionUID = -3160318945935927347L;

    /**
     * Create an DingoStringConcatOp. concat all input args.
     *
     * @param paras the parameters of the op
     */
    public DingoStringLocateOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String subString = (String) (values[0]);
        String inputStr = (String) (values[1]);

        if (subString.equals("")) {
            return Long.valueOf(1);
        }

        if (inputStr == null || inputStr.equals("")) {
            return Long.valueOf(0);
        }

        return new Long(inputStr.indexOf(subString) + 1);

    }

    public static long locateString(final String subString, final String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return -1;
        } else {
            return inputStr.indexOf(subString);
        }
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringLocateOp::new;
        }

        @Override
        public String name() {
            return "locate";
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringLocateOp.class.getMethod("locateString", String.class, String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
