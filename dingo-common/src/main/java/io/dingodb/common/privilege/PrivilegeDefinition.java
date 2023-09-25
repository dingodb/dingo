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

package io.dingodb.common.privilege;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.stream.Collectors;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PrivilegeDefinition {
    String user;
    String host;

    String grantorUser;

    String grantorHost;

    public String key() {
        return user + "#" + host;
    }

    List<String> privilegeList;

    public void setPrivilegeList(List<String> privilegeList) {
        this.privilegeList = privilegeList.stream().map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    public PrivilegeDefinition(String user, String host) {
        this.user = user;
        this.host = host;
    }

    public String getGrantor() {
        return grantorUser + "@" + grantorHost;
    }
}
