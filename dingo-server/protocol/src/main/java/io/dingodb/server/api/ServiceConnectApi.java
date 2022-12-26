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

package io.dingodb.server.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ServiceConnectApi {

    interface Service {
        CommonId id();

        Location leader();

        List<Location> getAll();
    }

    ServiceConnectApi INSTANCE = new ServiceConnectApi() {
        private final Map<CommonId, Service> services = new HashMap<>();

        @Override
        public void register(Service service) {
            services.put(service.id(), service);
        }

        @Override
        public void unregister(CommonId id) {
            services.remove(id);
        }

        @Override
        public Location leader(CommonId id) {
            return services.get(id).leader();
        }

        @Override
        public List<Location> getAll(CommonId id) {
            return services.get(id).getAll();
        }
    };

    default void register(Service service) {
        throw new UnsupportedOperationException();
    }

    default void unregister(CommonId id) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    Location leader(CommonId id);

    @ApiDeclaration
    List<Location> getAll(CommonId id);

}
