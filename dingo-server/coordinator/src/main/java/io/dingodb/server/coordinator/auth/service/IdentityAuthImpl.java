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

package io.dingodb.server.coordinator.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.Location;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.coordinator.CoordinatorSidebar;
import io.dingodb.server.coordinator.api.UserServiceApi;
import io.dingodb.verify.auth.IdentityAuth;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdentityAuthImpl implements IdentityAuth {

    private static final IdentityAuth INSTANCE = new IdentityAuthImpl();

    public UserServiceApi userServiceApi;

    public IdentityAuthImpl() {
        try {
            userServiceApi = new UserServiceApi();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AutoService(IdentityAuth.Provider.class)
    public static class IdentityAuthImplProvider implements IdentityAuth.Provider {

        @Override
        public <C> IdentityAuth<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public DingoRole getRole() {
        return DingoRole.COORDINATOR;
    }

    @Override
    public UserDefinition getUserDefinition(Authentication authentication) {
        String user = authentication.getUsername();
        String host = authentication.getHost();
        UserDefinition userDefinition = null;
        if (!CoordinatorSidebar.INSTANCE.isPrimary()) {
            Location location = CoordinatorSidebar.INSTANCE.getPrimary().location;
            io.dingodb.server.api.UserServiceApi remoteApi
                = ApiRegistry.getDefault().proxy(io.dingodb.server.api.UserServiceApi.class, location);
            userDefinition = remoteApi.getUserDefinition(user, host);
        } else {
            userDefinition = userServiceApi.getUserDefinition(user, host);
        }
        return userDefinition;
    }

    @Override
    public void cachePrivileges(Authentication authentication) {

    }
}
