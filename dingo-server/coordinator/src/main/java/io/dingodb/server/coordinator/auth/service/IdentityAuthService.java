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

package io.dingodb.server.coordinator.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.service.AuthService;
import io.dingodb.server.coordinator.api.SysInfoServiceApi;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.privilege.PrivilegeVerify;
import io.dingodb.verify.token.TokenManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IdentityAuthService implements AuthService<Authentication> {

    PrivilegeVerify privilegeVerify = new PrivilegeVerify();

    public SysInfoServiceApi sysInfoServiceApi;

    public IdentityAuthService() {
        sysInfoServiceApi = new SysInfoServiceApi();
    }

    private static final AuthService INSTANCE = new IdentityAuthService();

    @AutoService(AuthService.Provider.class)
    public static class IdentityAuthServiceProvider implements AuthService.Provider {
        @Override
        public <C> AuthService<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public String tag() {
        return "identity";
    }

    @Override
    public Authentication createAuthentication() {
        return null;
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        try {
            if (authentication == null) {
                return Certificate.builder().code(200).build();
            }
            if (!CoordinatorStateMachine.getInstance().isLeader()) {
                return Certificate.builder().code(100).build();
            }
            String user = authentication.getUsername();
            String host = authentication.getHost();
            String clientPassword = authentication.getPassword();

            PrivilegeGather privilegeGather = sysInfoServiceApi.getPrivilegeDef(user);
            UserDefinition userDef = privilegeVerify.matchUser(host, privilegeGather);

            if (user == null) {
                throw new Exception("user is null");
            }
            String plugin = userDef.getPlugin();
            String password = userDef.getPassword();
            String digestPwd = AlgorithmPlugin.digestAlgorithm(clientPassword, plugin);
            Certificate certificate = Certificate.builder().code(100).build();
            if (digestPwd.equals(password)) {
                Map<String, Object> clientInfo = new HashMap<>();
                clientInfo.put("username", user);
                clientInfo.put("host", host);
                TokenManager tokenManager = TokenManager.getInstance("0123456789");
                String token = tokenManager.createToken(clientInfo);
                clientInfo.put("token", token);
                certificate.setToken(token);
                certificate.setInfo(clientInfo);
                certificate.setPrivilegeGather(privilegeGather);
            } else {
                throw new Exception("password is wrong");
            }
            return certificate;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }
}
