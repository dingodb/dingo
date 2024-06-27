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

package io.dingodb.common.environment;

import java.util.Properties;

public class ClientIdentity {
    private final Properties properties = new Properties();

    public String getUser() {
        if (properties.containsKey("user")) {
            return properties.getProperty("user");
        } else {
            return null;
        }
    }

    public String getPassword() {
        if (properties.containsKey("password")) {
            return properties.getProperty("password");
        } else {
            return null;
        }
    }

    public void setInfo(String key, Object value) {
        properties.put(key, value);
    }

    public void putAll(Properties properties) {
        this.properties.putAll(properties);
    }
}
