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

package io.dingodb.net.netty.api;

import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.config.SecurityConfiguration;
import io.dingodb.net.Message;
import io.dingodb.net.error.ApiTerminateException;
import io.dingodb.net.netty.Channel;
import io.dingodb.net.netty.Constant;
import io.dingodb.net.service.AuthService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static io.dingodb.net.netty.Constant.API_ERROR;

public interface AuthProxyApi {

    class AuthProxyApiImpl implements AuthProxyApi {

        public final List<AuthService> services;

        private AuthProxyApiImpl() {
            List<AuthService> services = new ArrayList<>();
            Iterator<AuthService.Provider> providerIterator = ServiceLoader.load(AuthService.Provider.class).iterator();
            while (providerIterator.hasNext()) {
                services.add(providerIterator.next().get());
            }
            this.services = Collections.unmodifiableList(services);
        }

        @Override
        public Map<String, Object[]> auth(Channel channel, Map<String, ?> certificate) {
            Map<String, Object[]> result = new HashMap<>();
            if (!SecurityConfiguration.isAuth()) {
                return result;
            }
            try {
                for (AuthService service : services) {
                    Object cert = certificate.get(service.tag());
                    if (cert != null) {
                        result.put(service.tag(), new Object[]{cert, service.validate(cert)});
                    }
                }
                if (result.size() == 0) {
                    throw new Exception("identity and token: both authentication failed. ");
                }
                return result;
            } catch (Exception e) {
                channel.send(new Message(API_ERROR, ProtostuffCodec.write(e)), true);
                throw new ApiTerminateException(e,
                    "Auth failed from [%s], message: %s",
                    channel.remoteLocation().url(), e.getMessage()
                );
            }
        }
    }

    AuthProxyApiImpl INSTANCE = new AuthProxyApiImpl();

    /**
     * Authentication, throw exception if failed.
     *
     * @param certificate certificate
     */
    @ApiDeclaration(name = Constant.AUTH)
        Map<String, Object[]> auth(Channel channel, Map<String, ?> certificate);

    static Map<String, Object[]> auth(Channel channel) {
        Map<String, Object> certificates = new HashMap<>();
        if (SecurityConfiguration.isAuth()) {
            INSTANCE.services.forEach(service -> certificates.put(service.tag(), service.createCertificate()));
        }
        return ApiRegistryImpl.instance().proxy(AuthProxyApi.class, channel).auth(null, certificates);
    }

}
