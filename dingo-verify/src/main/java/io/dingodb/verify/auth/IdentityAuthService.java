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
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.service.AuthService;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.token.TokenManager;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class IdentityAuthService implements AuthService<Authentication> {

    public IdentityAuth identityAuth;

    Iterable<IdentityAuth.Provider> serviceProviders = ServiceLoader.load(IdentityAuth.Provider.class);

    public IdentityAuthService() {
        for (IdentityAuth.Provider identityAuthProvider : serviceProviders) {
            IdentityAuth identityAuth = identityAuthProvider.get();
            if (identityAuth.getRole() == Domain.role) {
                this.identityAuth = identityAuth;
            }
        }
    }

    private static final AuthService INSTANCE = new IdentityAuthService();

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
        if (userDef == null) {
            return false;
        }
        //todo
        if ("root".equals(userDef.getUser())) {
            return true;
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
    public Authentication createAuthentication() {
        Domain domain = Domain.INSTANCE;
        String user = (String) domain.getInfo("user");
        String host = getHost();
        String password = (String) domain.getInfo("password");
        if (StringUtils.isNotBlank(user)) {
            return Authentication.builder()
                .username(user)
                .host(host)
                .role(Domain.role)
                .password(password).build();
        } else {
            return null;
        }
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        if (identityAuth == null) {
            return null;
        }
        UserDefinition userDef = identityAuth.getUserDefinition(authentication);
        if (identification(userDef, authentication)) {
            identityAuth.cachePrivileges(authentication);
            return getCertificate(authentication.getUsername(), authentication.getHost());
        } else {
            throw new Exception(String.format("Access denied for user '%s'@'%s'", authentication.getUsername(),
                authentication.getHost()));
        }
    }
}
