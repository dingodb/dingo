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
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.mpu.core.MPURegister;
import io.dingodb.mpu.instruction.KVInstructions;

public interface KVApi {

    @ApiDeclaration
    default void set(CommonId mpu, CommonId core, byte[] key, byte[] value) {
        MPURegister.mpu(mpu).core(core).exec(KVInstructions.id, KVInstructions.SET_OC, key, value).join();
    }

    @ApiDeclaration
    default Object get(CommonId mpu, CommonId core, byte[] key) {
        return MPURegister.mpu(mpu).core(core).view(KVInstructions.id, KVInstructions.GET_OC, key);
    }

}
