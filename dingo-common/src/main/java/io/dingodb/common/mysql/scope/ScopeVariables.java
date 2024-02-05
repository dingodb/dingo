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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ScopeVariables {
    // load from store
    public static Properties globalVariables = new Properties();

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
}
