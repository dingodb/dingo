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

package io.dingodb.meta;

import io.dingodb.common.meta.Tenant;
import io.dingodb.common.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public interface TenantService {

    static TenantService getDefault() {
        return TenantServiceProvider.getDefault().get();
    }

    boolean createTenant(@NonNull Tenant tenant);

    void dropTenant(@NonNull String name);

    boolean updateTenant(@NonNull String oldName, String newName);

    Tenant getTenant(@NonNull String name);

    Tenant getTenant(long id);

    List<Tenant> listTenant();

    default Long getTenantId(@NonNull String name) {
        Tenant tenant = Optional.mapOrGet(listTenant(), __ -> __.stream().filter(t -> t.getName().equalsIgnoreCase(name)).findAny().orElse(null), () -> null);
        return tenant == null ? null : tenant.getId();
    }
}
