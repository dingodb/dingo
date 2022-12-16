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
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.meta.SysInfoService;
import io.dingodb.meta.SysInfoServiceProvider;
import io.dingodb.verify.auth.IdentityAuth;
import io.dingodb.verify.auth.IdentityAuthService;
import io.dingodb.verify.privilege.PrivilegeVerify;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class IdentityAuthImpl implements IdentityAuth {

    private SysInfoService sysInfoService;

    private static final IdentityAuth INSTANCE = new IdentityAuthImpl();

    @AutoService(IdentityAuth.Provider.class)
    public static class IdentityAuthImplProvider implements IdentityAuth.Provider {

        @Override
        public <C> IdentityAuth<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public DingoRole getRole() {
        return DingoRole.JDBC;
    }

    @Override
    public UserDefinition getUserDefinition(Authentication authentication) {
        if (sysInfoService == null) {
            sysInfoService = SysInfoServiceProvider.getRoot();
        }
        String user = authentication.getUsername();
        String host = authentication.getHost();
        List<UserDefinition> userDefinitionList = sysInfoService.getUserDefinition(user);
        UserDefinition userDef = PrivilegeVerify.matchUser(host, userDefinitionList);
        return userDef;
    }

    @Override
    public void cachePrivileges(Authentication authentication) {
        String user = authentication.getUsername();
        String host = authentication.getHost();
        PrivilegeGather privilegeGather = sysInfoService.getPrivilegeDef(null, user, host);
        Domain.INSTANCE.privilegeGatherMap.put(privilegeGather.key(),
            privilegeGather);
        log.info("cache privileges:" + Domain.INSTANCE.privilegeGatherMap);
    }
}
