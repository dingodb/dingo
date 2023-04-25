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

package io.dingodb.driver.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.verify.auth.IdentityAuth;
import io.dingodb.verify.service.UserService;
import io.dingodb.verify.service.UserServiceProvider;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdentityAuthImpl implements IdentityAuth {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private UserService userService;
    private static final IdentityAuth INSTANCE = new IdentityAuthImpl();

    @AutoService(IdentityAuth.Provider.class)
    public static class IdentityAuthImplProvider implements IdentityAuth.Provider {

        @Override
        public IdentityAuth get() {
            return INSTANCE;
        }
    }

    @Override
    public DingoRole getRole() {
        return DingoRole.JDBC;
    }

    @Override
    public UserDefinition getUserDefinition(Authentication authentication) {
        if (userService == null) {
            userService = UserServiceProvider.getRoot();
        }
        String user = authentication.getUsername();
        String host = authentication.getHost();
        return userService.getUserDefinition(user, host);
    }

    @Override
    public void cachePrivileges(Authentication authentication) {
        String user = authentication.getUsername();
        String host = authentication.getHost();
        PrivilegeGather privilegeGather = userService.getPrivilegeDef(user, host);
        env.getPrivilegeGatherMap().put(privilegeGather.key(),
            privilegeGather);
        log.info("cache privileges:" + env.getPrivilegeGatherMap());
    }
}
