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

import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Configuration {

    private static final Configuration INSTANCE;

    static {
        try {
            DingoConfiguration.instance().setServer(Configuration.class);
            INSTANCE = DingoConfiguration.instance().getServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration instance() {
        return INSTANCE;
    }

    private String coordinators;
    private int port = 8765;

    public static String coordinators() {
        return INSTANCE.coordinators;
    }
    public static int port() {return INSTANCE.port;};

}
