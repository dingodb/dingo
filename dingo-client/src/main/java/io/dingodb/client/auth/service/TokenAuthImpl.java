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

package io.dingodb.client.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.verify.auth.TokenAuth;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenAuthImpl implements TokenAuth {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private static final TokenAuth INSTANCE = new TokenAuthImpl();

    @AutoService(TokenAuth.Provider.class)
    public static class TokenAuthImplProvider implements TokenAuth.Provider {

        @Override
        public <C> TokenAuth<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public DingoRole getRole() {
        return DingoRole.SDK_CLIENT;
    }

    @Override
    public void cachePrivileges(Authentication authentication) {

    }

    public String getAuthToken() {
        return null;
    }

    //private static String getToken() {
    //    try {
    //        log.info("sdk token get coordinator leader:" + CoordinatorConnector.getDefault().get());
    //        Map<String, Object[]> authContent = NetService.getDefault().auth(CoordinatorConnector.getDefault().get());
    //        if (authContent != null) {
    //            Object[] identityRet = authContent.get("identity");
    //            Certificate certificate = (Certificate) identityRet[1];
    //            if (certificate != null) {
    //                String token = certificate.getToken();
    //                if (StringUtils.isNotBlank(token)) {
    //                    //ReloadHandler.handler.registryFlushChannel();
    //                    //loadPrivileges();
    //                }
    //                return token;
    //            }
    //        }
    //    } catch (Exception e) {
    //        log.error(e.getMessage(), e);
    //    }
    //    return "";
    //}
}
