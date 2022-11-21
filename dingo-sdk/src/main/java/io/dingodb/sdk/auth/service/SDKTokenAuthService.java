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
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.service.AuthService;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.ServiceLoader;

@Slf4j
public class SDKTokenAuthService implements AuthService<Authentication> {
    private final static AuthService INSTANCE = new SDKTokenAuthService();

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @AutoService(AuthService.Provider.class)
    public static class SDKTokenAuthServiceProvider implements AuthService.Provider {

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
        // sdk token auth depend on identity auth by coordinator
        if (Domain.role == DingoRole.SDK_CLIENT && CoordinatorConnector.defaultConnector().verify()) {
            String token = getToken();
            log.info("sdk token auth:" + token);
            if (StringUtils.isNotBlank(token)) {
                Authentication authentication = Authentication.builder().token(token).role(Domain.role).build();
                return authentication;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        return Certificate.builder().code(200).build();
    }

    private String getToken() {
        try {
            log.info("sdk token get coordinator leader:" + CoordinatorConnector.defaultConnector().get());
            Map<String, Object[]> authContent = netService.auth(CoordinatorConnector.defaultConnector().get());
            if (authContent != null) {
                Object[] identityRet = authContent.get("identity");
                Certificate certificate = (Certificate) identityRet[0];
                if (certificate != null) {
                    String token = certificate.getToken();
                    if (StringUtils.isNotBlank(token)) {
                        // if sdk identity auth success, then mapping return privilege to sdk invoke by
                        Map<String, Boolean[]> sdkPrivilege = certificate.privilege2SDK();
                        identityRet[1] = sdkPrivilege;
                    }
                    return token;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

}
