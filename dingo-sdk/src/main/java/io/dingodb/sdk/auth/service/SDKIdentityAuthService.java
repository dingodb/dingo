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

package io.dingodb.sdk.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.net.service.AuthService;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class SDKIdentityAuthService implements AuthService<Authentication> {

    private final static AuthService INSTANCE = new SDKIdentityAuthService();

    @AutoService(AuthService.Provider.class)
    public static class SDKIdentityAuthServiceProvider implements AuthService.Provider {

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
        if (Domain.role == DingoRole.SDK_CLIENT) {
            Domain domain = Domain.getInstance();
            String user = domain.getInfo("user").toString();
            String host = getHost();
            String password = domain.getInfo("password").toString();

            Authentication authentication = Authentication.builder()
                .username(user)
                .host(host)
                .role(Domain.role)
                .password(password).build();
            return authentication;
        } else {
            return null;
        }
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        return Certificate.builder().code(200).build();
    }

    private String getHost() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }
}
