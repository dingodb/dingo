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

package io.dingodb.client.configuration;

import org.apache.commons.lang3.StringUtils;

public class KeyConfig {
    private String field;
    private String getter;
    private String setter;

    public String getField() {
        return field;
    }

    public String getGetter() {
        return getter;
    }

    public String getSetter() {
        return setter;
    }

    public boolean isGetter(String methodName) {
        return (!StringUtils.isBlank(this.getter)) && this.getter.equals(methodName);
    }

    public boolean isSetter(String methodName) {
        return (!StringUtils.isBlank(this.setter)) && this.setter.equals(methodName);
    }
}
