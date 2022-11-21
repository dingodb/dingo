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

package io.dingodb.web.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.domain.Domain;
import io.dingodb.net.service.AuthService;
import io.dingodb.verify.token.TokenManager;

import java.util.HashMap;
import java.util.Map;

public class WebTokenAuthService implements AuthService<Authentication> {
    private final static AuthService INSTANCE = new WebTokenAuthService();

    @AutoService(Provider.class)
    public static class WebTokenAuthServiceProvider implements Provider {

        @Override
        public <C> AuthService<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public String tag() {
        return "token";
    }

    @Override
    public Authentication createAuthentication() {
        TokenManager tokenManager = TokenManager.getInstance("0123456789");
        Map<String, Object> map = new HashMap<>();
        map.put("inner", "dingo");
        String token =  tokenManager.createToken(map);
        return Authentication.builder().role(Domain.role).token(token).build();
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        return Certificate.builder().code(200).build();
    }
}
