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

import io.dingodb.sdk.common.DingoClientException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Getter
@Setter
public class ColumnConfig {
    private String name;
    private String field;
    private Boolean useAccessors;
    private String getter;
    private String setter;
    private Integer ordinal;
    private EmbedConfig embed;
    private Boolean exclude;

    public void validate(String className) {
        if (StringUtils.isBlank(this.name) && StringUtils.isBlank(this.field)) {
            throw new DingoClientException("Configuration for class " + className
                + " defines a column which contains neither a name nor a field");
        }
    }

    public String getDerivedName() {
        if (!StringUtils.isBlank(this.name)) {
            return this.name;
        }
        return this.field;
    }


    public Boolean isExclude() {
        return exclude;
    }
}
