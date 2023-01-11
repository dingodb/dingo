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
import io.dingodb.common.privilege.PrivilegeType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;

import static io.dingodb.server.protocol.CommonIdConstant.PRIVILEGE_IDENTIFIER;

@Getter
@Setter
@Builder
@ToString
@FieldNameConstants(asEnum = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Privilege implements Meta {

    private CommonId subjectId;

    private PrivilegeType privilegeType;

    private CommonId id;

    private String user;

    private String host;

    private CommonId schema;

    private CommonId table;

    private Integer privilegeIndex;

    private long createTime;

    private long updateTime;

    @Override
    public void setId(CommonId id) {
        this.id = id;
    }

    @Override
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public CommonId getId() {
        return id;
    }

    @Override
    public String getName() {
        return user + "@" + host;
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getUpdateTime() {
        return 0;
    }

    public byte[] identifier() {
        if (privilegeType == PrivilegeType.SCHEMA) {
            return PRIVILEGE_IDENTIFIER.schemaPrivilege;
        } else if (privilegeType == PrivilegeType.TABLE) {
            return PRIVILEGE_IDENTIFIER.tablePrivilege;
        } else if (privilegeType == PrivilegeType.USER) {
            return PRIVILEGE_IDENTIFIER.user;
        } else {
            throw new RuntimeException("grant is error");
        }
    }

    public String privilegeKey() {
        StringBuilder privilegeKey = new StringBuilder();
        privilegeKey.append(user);
        privilegeKey.append("#");
        privilegeKey.append(host);
        if (privilegeType == PrivilegeType.USER) {
            return privilegeKey.toString();
        } else if (privilegeType == PrivilegeType.SCHEMA) {
            privilegeKey.append("#");
            return privilegeKey.append(schema).toString();
        } else if (privilegeType == PrivilegeType.TABLE) {
            privilegeKey.append("#");
            return privilegeKey.append(schema).append("#").append(table).toString();
        } else {
            throw new RuntimeException("grant is error");
        }
    }
}
