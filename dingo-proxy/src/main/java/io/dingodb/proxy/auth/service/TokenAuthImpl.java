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

package io.dingodb.proxy.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.verify.auth.TokenAuth;
import io.dingodb.verify.token.TokenManager;
import io.dingodb.proxy.bean.SecurityCipher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TokenAuthImpl implements TokenAuth {

    TokenManager tokenManager;

    public static final TokenAuth INSTANCE = new TokenAuthImpl();

    @AutoService(Provider.class)
    public static class TokenAuthServiceProvider implements Provider {

        @Override
        public <C> TokenAuth<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public DingoRole getRole() {
        return null;
    }

    public String getAuthToken() {
        if (tokenManager == null) {
            String keyPath = SecurityCipher.securityCipher.keyPath;
            String keyPass = SecurityCipher.securityCipher.keyPass;
            String storePass = SecurityCipher.securityCipher.storePass;
            String alias = SecurityCipher.securityCipher.alias;
            String issuer = SecurityCipher.securityCipher.issuer;
            tokenManager = TokenManager.getInstance(keyPath, keyPass,
                storePass, alias, issuer);
            log.info("keyPath:" + keyPath + ", keyPass:" + keyPass
                + ", storePass:" + storePass
                + ", alias:" + alias + ", issuer:" + issuer + "");
        }
        String token =  tokenManager.createInnerToken();
        return token;
    }

    @Override
    public void cachePrivileges(Authentication authentication) {
    }
}
