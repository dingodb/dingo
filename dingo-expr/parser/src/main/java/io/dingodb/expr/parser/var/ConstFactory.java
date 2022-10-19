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

package io.dingodb.expr.parser.var;

import io.dingodb.expr.runtime.RtConst;

import java.util.HashMap;
import java.util.Map;

public final class ConstFactory {
    public static final ConstFactory INS = new ConstFactory();

    private final Map<String, RtConst> constDefinitions;

    private ConstFactory() {
        constDefinitions = new HashMap<>(1);
        constDefinitions.put("TAU", RtConst.TAU);
        constDefinitions.put("E", RtConst.E);
    }

    /**
     * Get a RtConst by its name.
     *
     * @param name the name
     * @return the RtConst
     */
    public RtConst getConst(String name) {
        return constDefinitions.get(name);
    }
}
