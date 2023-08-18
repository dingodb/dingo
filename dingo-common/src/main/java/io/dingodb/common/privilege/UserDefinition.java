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

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.sql.Timestamp;

@Setter
@Getter
@NoArgsConstructor
@ToString
public class UserDefinition extends PrivilegeDefinition {
    private String plugin;
    private String password;
    private String requireSsl;

    private String lock;

    private Object passwordExpire;

    private Timestamp pwdLastChange;

    private Object expireDays;

    Boolean[] privileges;

    @Builder(toBuilder = true)
    public UserDefinition(String user, String host, String plugin,
                          String password, String requireSsl,
                          String lock,
                          Object expireDays) {
        super(user, host);
        this.plugin = plugin;
        this.password = password;
        this.requireSsl = requireSsl;
        this.lock = lock;
        this.expireDays = expireDays;
    }

    public String getKey() {
        return user + "#" + host;
    }

}
