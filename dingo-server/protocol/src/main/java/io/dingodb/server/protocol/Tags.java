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

package io.dingodb.server.protocol;

public class Tags {

    public static final String LISTEN_SERVICE_LEADER = "LISTEN_SERVICE";

    public static final String LISTEN_REGISTRY_FLUSH = "LISTEN_REGISTRY_FLUSH";
    public static final String LISTEN_RELOAD_PRIVILEGES = "LISTEN_RELOAD_PRIVILEGES";

    public static final String LISTEN_RELOAD_PRIVILEGE_DICT = "LISTEN_RELOAD_PRIVILEGE_DICT";

    private Tags() {
    }
}
