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

package io.dingodb.verify.plugin;

import java.util.HashMap;
import java.util.Map;

public class AlgorithmPlugin {
    public static Map<String, AlgorithmPlugin> algorithmPluginMap;

    static {
        algorithmPluginMap = new HashMap<>();
        algorithmPluginMap.put("mysql_native_password", new MysqlNativePassword());
    }

    public static String digestAlgorithm(String password, String plugin) {
        AlgorithmPlugin algorithmPlugin = AlgorithmPlugin.algorithmPluginMap.get(plugin);
        return algorithmPlugin.getEncodedPassword(password);
    }

    public String getEncodedPassword(String password) {
        return null;
    }
}
