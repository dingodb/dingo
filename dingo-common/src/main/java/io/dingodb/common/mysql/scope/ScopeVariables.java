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

public class ScopeVariables {
    public static Map<String, String> globalVariables= new HashMap();

    public static Map<String, String> sessionVariables= new HashMap();
    static {
        globalVariables.put("version_comment", "Ubuntu");
        globalVariables.put("wait_timeout", "30");
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


        sessionVariables.put("version_comment", "Ubuntu");
        sessionVariables.put("wait_timeout", "30");
        sessionVariables.put("transaction_isolation", "");
        sessionVariables.put("time_zone", "SYSTEM");
        sessionVariables.put("system_time_zone", "UTC");
        sessionVariables.put("sql_mode", "");
        sessionVariables.put("query_cache_type", "OFF");
        sessionVariables.put("query_cache_size", "16777216");
        sessionVariables.put("performance_schema", "1");
        sessionVariables.put("net_write_timeout", "60");
        sessionVariables.put("max_allowed_packet", "16777216");
        sessionVariables.put("lower_case_table_names", "0");
        sessionVariables.put("license", "GPL");

        sessionVariables.put("interactive_timeout", "28800");
        sessionVariables.put("init_connect", "");
        sessionVariables.put("collation_connection", "utf8_general_ci");
        sessionVariables.put("collation_server", "latin1_swedish_ci");
        sessionVariables.put("character_set_server", "latin1");
        sessionVariables.put("character_set_results", "utf8");
        sessionVariables.put("character_set_connection", "utf8");
        sessionVariables.put("character_set_client", "utf8");
        sessionVariables.put("auto_increment_increment", "1");
        sessionVariables.put("transaction_read_only", "0");

    }
}
