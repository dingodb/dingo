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

package io.dingodb.server.executor.common;

import io.dingodb.common.CommonId;

public class DingoCommonId implements io.dingodb.sdk.common.DingoCommonId {

    private final CommonId commonId;

    public DingoCommonId(CommonId commonId) {
        this.commonId = commonId;
    }

    @Override
    public Type type() {
        return getType();
    }

    @Override
    public long parentId() {
        return commonId.domain;
    }

    @Override
    public long entityId() {
        return commonId.seq;
    }

    private Type getType() {
        switch (commonId.type) {
            case TABLE:
                return Type.valueOf(EntityType.ENTITY_TYPE_TABLE.name());
            case SCHEMA:
                return Type.valueOf(EntityType.ENTITY_TYPE_SCHEMA.name());
            case DISTRIBUTION:
                return Type.valueOf(EntityType.ENTITY_TYPE_PART.name());
            default:
                throw new RuntimeException("Unsupported type");
        }
    }

    public enum EntityType {
        ENTITY_TYPE_TABLE(0),
        ENTITY_TYPE_SCHEMA(1),
        ENTITY_TYPE_PART(2);

        private final int code;

        EntityType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }
}
