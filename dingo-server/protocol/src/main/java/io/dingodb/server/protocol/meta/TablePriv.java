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

package io.dingodb.server.protocol.meta;

import io.dingodb.common.CommonId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;

@Setter
@Getter
@NoArgsConstructor
@Builder
@ToString
@FieldNameConstants(asEnum = true)
@AllArgsConstructor
@EqualsAndHashCode
public class TablePriv implements Meta {

    private CommonId id;

    private String user;

    private String host;

    private CommonId schema;

    private CommonId table;

    @Override
    public void setId(CommonId id) {
        this.id = id;
    }

    @Override
    public void setCreateTime(long createTime) {

    }

    @Override
    public void setUpdateTime(long updateTime) {

    }

    @Override
    public CommonId getId() {
        return id;
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getUpdateTime() {
        return 0;
    }

    public String getKey() {
        StringBuilder schemaPrivKey = new StringBuilder();
        return schemaPrivKey.append(user)
            .append("#").append(host)
            .append("#").append(schema)
            .append("#").append(table).toString();
    }
}
