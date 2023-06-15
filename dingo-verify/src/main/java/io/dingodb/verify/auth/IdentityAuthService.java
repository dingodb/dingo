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

package io.dingodb.verify.auth;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.service.AuthService;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.token.TokenManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

@Slf4j
public class IdentityAuthService implements AuthService<Authentication> {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public IdentityAuth identityAuth;

    Iterable<IdentityAuth.Provider> serviceProviders = ServiceLoader.load(IdentityAuth.Provider.class);

    public IdentityAuthService() {
        for (IdentityAuth.Provider identityAuthProvider : serviceProviders) {
            this.identityAuth = identityAuthProvider.get();
            log.info("IdentityAuth: {}", identityAuth);
        }
    }

    public static final AuthService INSTANCE = new IdentityAuthService();

    @AutoService(AuthService.Provider.class)
    public static class IdentityAuthServiceProvider implements AuthService.Provider {

        @Override
        public <C> AuthService<C> get() {
            return INSTANCE;
        }
    }

    public Certificate getCertificate(String user, String host) {
        Map<String, Object> clientInfo = new HashMap<>();
        clientInfo.put("user", user);
        clientInfo.put("host", host);
        String token = TokenManager.INSTANCE.createToken(clientInfo);
        return Certificate.builder().code(100).token(token).build();
    }

    public boolean identification(UserDefinition userDef, Authentication authentication) {
        if (authentication.getRole() == DingoRole.EXECUTOR && env.getRole() == DingoRole.EXECUTOR) {
            return true;
        }
        if (userDef == null) {
            return false;
        }
        String plugin = userDef.getPlugin();
        String password = userDef.getPassword();
        String digestPwd = AlgorithmPlugin.digestAlgorithm(authentication.getPassword(), plugin);
        return digestPwd.equals(password);
    }

    public String getHost() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    @Override
    public String tag() {
        return "identity";
    }

    @Override
    public Authentication createCertificate() {
        String user = env.getUser();
        String host = getHost();
        String password = env.getPassword();
        if (StringUtils.isNotBlank(user)) {
            return Authentication.builder()
                .username(user)
                .host(host)
                .role(env.getRole())
                .password(password).build();
        } else {
            return null;
        }
    }

    @Override
    public Object validate(Authentication certificate) throws Exception {
        if (identityAuth == null) {
            return null;
        }
        UserDefinition userDef = identityAuth.getUserDefinition(certificate);
        if (identification(userDef, certificate)) {
            identityAuth.cachePrivileges(certificate);
            return getCertificate(certificate.getUsername(), certificate.getHost());
        } else {
            throw new Exception(String.format("Access denied for user '%s'@'%s'", certificate.getUsername(),
                certificate.getHost()));
        }
    }
}
