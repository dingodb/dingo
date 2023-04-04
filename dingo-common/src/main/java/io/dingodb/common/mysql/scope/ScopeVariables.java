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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ScopeVariables {
    public static Properties globalVariables = new Properties();

    public static Properties sessionVariables = new Properties();

    static {
        globalVariables.put("version_comment", "Ubuntu");
        globalVariables.put("wait_timeout", "28800");
        globalVariables.put("transaction_isolation", "");
        globalVariables.put("time_zone", "SYSTEM");
        globalVariables.put("system_time_zone", "UTC");
        globalVariables.put("sql_mode", "");
        globalVariables.put("query_cache_type", "OFF");
        globalVariables.put("query_cache_size", "16777216");
        globalVariables.put("performance_schema", "1");
        globalVariables.put("net_write_timeout", "60");
        globalVariables.put("max_allowed_packet", "16777216");
        globalVariables.put("lower_case_table_names", "0");
        globalVariables.put("license", "GPL");

        globalVariables.put("interactive_timeout", "28800");
        globalVariables.put("init_connect", "");
        globalVariables.put("collation_connection", "utf8_general_ci");
        globalVariables.put("collation_server", "latin1_swedish_ci");
        globalVariables.put("character_set_server", "latin1");
        globalVariables.put("character_set_results", "utf8");
        globalVariables.put("character_set_connection", "utf8");
        globalVariables.put("character_set_client", "utf8");
        globalVariables.put("auto_increment_increment", "1");

    }
}
