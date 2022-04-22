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

package io.dingodb.calcite;

import io.dingodb.func.DingoFuncProvider;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class DingoFunctions {
    private DingoFunctions() {
    }

    private static DingoFunctions instance;

    private List<NameMethodMapping> dingoUserFunctions = new ArrayList<>();

    public static synchronized DingoFunctions getInstance() {
        if (instance == null) {
            instance = new DingoFunctions();
            instance.initNameMethodMapping();
        }
        return instance;
    }

    private void initNameMethodMapping() {
        ServiceLoader.load(DingoFuncProvider.class).iterator().forEachRemaining(
            f -> f.methods().forEach(m -> {
                for (String name: f.name()) {
                    NameMethodMapping value = new NameMethodMapping(name, m);
                    dingoUserFunctions.add(value);
                }
            }));
    }

    public List<NameMethodMapping> getDingoFunctions() {
        return dingoUserFunctions;
    }

    public Method getDingoFunction(String name) {

        /**
         * return the first function name default.
         */
        for (NameMethodMapping value: dingoUserFunctions) {
            if (value.getName().equalsIgnoreCase(name)) {
                return value.getMethod();
            }
        }
        return null;
    }

    @AllArgsConstructor
    @Getter
    public static class NameMethodMapping {
        private String name;
        private Method method;
    }

    // TODO: will be deleted
    public static void main(String[] args) {
        List<NameMethodMapping> result = DingoFunctions.getInstance().getDingoFunctions();
        for (NameMethodMapping value: result) {
            try {
                Method method = value.getMethod();
                System.out.println(method.getName());
                if ((method.getGenericParameterTypes().length == 0)
                    && method.getReturnType().getTypeName().equals("java.lang.Long")) {
                    Long timestamp = (Long)(method.invoke(null));
                    System.out.println(timestamp);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
