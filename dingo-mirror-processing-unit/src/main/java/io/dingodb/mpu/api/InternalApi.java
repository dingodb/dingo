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
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.MPURegister;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.protocol.SelectReturn;
import io.dingodb.mpu.protocol.SyncChannel;

import static io.dingodb.mpu.Constant.API;

public interface InternalApi {

    static void load() {
        API.register(InternalApi.class, new InternalApi() {});
    }

    static Optional<Core> core(CommonId coreId) {
        return Optional.ofNullable(MPURegister.get(coreId));
    }

    static InternalApi instance(Location location) {
        return API.proxy(InternalApi.class, location, 3);
    }

    @ApiDeclaration
    default void ping() {
    }

    static void ping(Location location) {
        instance(location).ping();
    }

    @ApiDeclaration
    default SelectReturn connectMirror(SyncChannel syncChannel) {
        return core(syncChannel.primary.coreId)
            .map(core -> core.getVCore().connectFromPrimary(syncChannel))
            .orElse(SelectReturn.ERROR);
    }

    static SelectReturn connectMirror(Location location, SyncChannel syncChannel) {
        return instance(location).connectMirror(syncChannel);
    }

    @ApiDeclaration
    default SelectReturn askPrimary(CoreMeta meta, long clock) {
        return core(meta.coreId)
            .map(core -> core.getVCore().askPrimary(meta, clock))
            .orElse(SelectReturn.ERROR);
    }

    static SelectReturn askPrimary(Location location, CoreMeta meta, long clock) {
        try {
            return instance(location).askPrimary(meta, clock);
        } catch (Exception e) {
            return SelectReturn.ERROR;
        }
    }

    @ApiDeclaration
    default boolean isPrimary(CommonId coreId) {
        return core(coreId)
            .map(core -> core.getVCore().isPrimary())
            .orElse(false);
    }

    static boolean isPrimary(Location location, CommonId coreId) {
        try {
            return instance(location).isPrimary(coreId);
        } catch (Exception e) {
            return false;
        }
    }

    @ApiDeclaration
    default void requestConnect(CoreMeta mirror) {
        core(mirror.coreId)
            .ifPresent(core -> core.getVCore().requestConnect(mirror));
    }

    static void requestConnect(Location location, CoreMeta mirror) {
        instance(location).requestConnect(mirror);
    }

    @ApiDeclaration
    default long askClock(CommonId coreId) {
        return core(coreId).get().getVCore().clock();
    }

    static long askClock(Location location, CommonId coreId) {
        return instance(location).askClock(coreId);
    }

}
