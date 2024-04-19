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

package io.dingodb.common.mysql.scope;

import io.dingodb.common.metrics.DingoMetrics;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class ScopeVariables {
    // load from store
    @Getter
    private static Properties globalVariables = new Properties();

    public static final List<String> immutableVariables = new ArrayList<>();

    public static final List<String> characterSet = new ArrayList<>();

    static {
        immutableVariables.add("version_comment");
        immutableVariables.add("version");
        immutableVariables.add("version_compile_os");
        immutableVariables.add("version_compile_machine");
        immutableVariables.add("license");
        immutableVariables.add("default_storage_engine");
        immutableVariables.add("have_openssl");
        immutableVariables.add("have_ssl");
        immutableVariables.add("have_statement_timeout");
        immutableVariables.add("last_insert_id");
        immutableVariables.add("@begin_transaction");

        characterSet.add("utf8mb4");
        characterSet.add("utf8");
        characterSet.add("utf-8");
        characterSet.add("gbk");
        characterSet.add("latin1");
    }

    private ScopeVariables() {
    }

    public static boolean globalVarEmpty() {
        return globalVariables.isEmpty();
    }

    public static synchronized void setGlobalVariable(String key, Object value) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if ("metric_log_enable".equalsIgnoreCase(key)) {
            if (value == null) {
                return;
            }
            metricReporter(value.toString());
        }
        globalVariables.put(key, value);
    }

    public static synchronized String getGlobalVar(String key, String defaultVal) {
        return globalVariables.getProperty(key, defaultVal);
    }

    public static synchronized void putAllGlobalVar(Map<String, String> globalVariableMap) {
        if (globalVariableMap.containsKey("metric_log_enable")) {
            String metricLogEnable = globalVariableMap.get("metric_log_enable");
            metricReporter(metricLogEnable);
        }
        globalVariables.putAll(globalVariableMap);
    }

    private static synchronized void metricReporter(String metricLogEnable) {
        if ("on".equalsIgnoreCase(metricLogEnable)) {
            DingoMetrics.startReporter();
        } else if ("off".equalsIgnoreCase(metricLogEnable)) {
            DingoMetrics.stopReporter();
        }
    }

    public static synchronized boolean containsGlobalVarKey(String key) {
        return globalVariables.containsKey(key);
    }
}
