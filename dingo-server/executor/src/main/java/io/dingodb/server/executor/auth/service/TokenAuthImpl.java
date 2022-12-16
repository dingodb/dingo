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

package io.dingodb.server.executor.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.meta.SysInfoService;
import io.dingodb.meta.SysInfoServiceProvider;
import io.dingodb.verify.auth.TokenAuth;
import io.dingodb.verify.auth.TokenAuthService;
import io.dingodb.verify.token.TokenManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TokenAuthImpl implements TokenAuth {

    private static final TokenAuth INSTANCE = new TokenAuthImpl();

    private SysInfoService sysInfoService;

    @AutoService(TokenAuth.Provider.class)
    public static class TokenAuthServiceProvider implements TokenAuth.Provider {

        @Override
        public <C> TokenAuth<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public DingoRole getRole() {
        return DingoRole.EXECUTOR;
    }

    public String getAuthToken() {
        String token =  TokenManager.INSTANCE.createInnerToken();
        return token;
    }

    @Override
    public void cachePrivileges(Authentication authentication) {
        if (sysInfoService == null) {
            sysInfoService = SysInfoServiceProvider.getRoot();
        }
        String user = authentication.getUsername();
        String host = authentication.getHost();
        PrivilegeGather privilegeGather = sysInfoService.getPrivilegeDef(null, user, host);
        Domain.INSTANCE.privilegeGatherMap.put(privilegeGather.key(),
            privilegeGather);
        log.info("sdk to executor token auth success: user:" + user + ", host:" + host + ", privileges:"
            + Domain.INSTANCE.privilegeGatherMap);
    }
}
