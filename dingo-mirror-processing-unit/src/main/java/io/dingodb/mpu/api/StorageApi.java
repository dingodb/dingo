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

package io.dingodb.mpu.api;

import io.dingodb.common.CommonId;
import io.dingodb.net.api.annotation.ApiDeclaration;

import static io.dingodb.mpu.Constant.API;

public interface StorageApi {

    static void load() {
        API.register(StorageApi.class, new StorageApi() {});
    }

    @ApiDeclaration
    default String transferBackup(CommonId mpuId, CommonId coreId) {
        return InternalApi.core(mpuId, coreId).storage.receiveBackup();
    }

    @ApiDeclaration
    default void applyBackup(CommonId mpuId, CommonId coreId) {
        InternalApi.core(mpuId, coreId).storage.applyBackup();
    }

}
