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
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.service.AuthService;
import io.dingodb.server.api.SysInfoServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.privilege.PrivilegeVerify;
import io.dingodb.verify.token.TokenManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

@Slf4j
public class DriverIdentityAuthService implements AuthService<Authentication> {
    PrivilegeVerify privilegeVerify = new PrivilegeVerify();

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private SysInfoServiceApi sysInfoServiceApi;

    public DriverIdentityAuthService() {
        this.sysInfoServiceApi = netService.apiRegistry().proxy(SysInfoServiceApi.class,
            CoordinatorConnector.defaultConnector());
    }

    public static final AuthService INSTANCE = new DriverIdentityAuthService();

    @AutoService(Provider.class)
    public static class DriverIdentityAuthServiceProvider implements Provider {
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
            if (Domain.role == DingoRole.DRIVER) {
                log.info("authentication:" + authentication);
                String user = authentication.getUsername();
                String host = authentication.getHost();
                String clientPassword = authentication.getPassword();

                List<UserDefinition> userDefinitionList = sysInfoServiceApi.getUserDefinition(user);
                UserDefinition userDef = privilegeVerify.matchUser(host, userDefinitionList);

                if (user == null) {
                    throw new Exception("user is null");
                }
                String plugin = userDef.getPlugin();
                String password = userDef.getPassword();
                String digestPwd = AlgorithmPlugin.digestAlgorithm(clientPassword, plugin);
                Certificate certificate = Certificate.builder().code(100).build();
                if (digestPwd.equals(password)) {
                    PrivilegeGather privilegeGather = sysInfoServiceApi.getPrivilegeDef(user);

                    Map<String, Object> clientInfo = new HashMap<>();
                    clientInfo.put("username", user);
                    clientInfo.put("host", host);
                    TokenManager tokenManager = TokenManager.getInstance("0123456789");
                    String token = tokenManager.createToken(clientInfo);
                    clientInfo.put("token", token);

                    certificate.setToken(token);
                    certificate.setPrivilegeGather(privilegeGather);
                    certificate.setInfo(clientInfo);
                } else {
                    throw new Exception("password is wrong");
                }
                return certificate;
            } else {
                return Certificate.builder().code(200).build();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }
}
