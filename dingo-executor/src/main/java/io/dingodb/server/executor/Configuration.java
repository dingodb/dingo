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

package io.dingodb.server.executor;

import io.dingodb.common.config.DingoConfiguration;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Configuration {

    public static final String KEY = "server";
    private static final Configuration INSTANCE = DingoConfiguration.instance().getConfig(KEY, Configuration.class);

    public static Configuration instance() {
        return INSTANCE;
    }

    private String coordinators;

    private String user;
    private String keyring;
    private String resourceTag;

    private Integer mysqlPort = 3307;

    private Long autoAnalyzeCommitSize = 0L;

    public static String coordinators() {
        return INSTANCE.coordinators;
    }

    public static String user() {
        return INSTANCE.user;
    }

    public static String keyring() {
        return INSTANCE.keyring;
    }

    public static String resourceTag() {
        return INSTANCE.resourceTag;
    }

    public static Integer mysqlPort() {
        return INSTANCE.mysqlPort;
    }

    public static Long autoAnalyzeCommitSize() {
        return INSTANCE.autoAnalyzeCommitSize;
    }
}
