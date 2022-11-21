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
import io.dingodb.net.service.AuthService;

public class JDBCAuthService implements AuthService<Authentication> {

    private static final AuthService INSTANCE = new JDBCAuthService();

    @AutoService(AuthService.Provider.class)
    public static class JDBCAuthServiceProvider implements AuthService.Provider {

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
        if (Domain.role == DingoRole.JDBC_CLIENT) {
            Domain domain = Domain.getInstance();
            String user = domain.getInfo("user").toString();
            String host = domain.getInfo("host").toString();
            String password = domain.getInfo("password").toString();

            Authentication authentication = Authentication.builder()
                .username(user)
                .host(host)
                .password(password).role(domain.role).build();
            return authentication;
        } else {
            return null;
        }
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        return Certificate.builder().code(200).build();
    }
}
