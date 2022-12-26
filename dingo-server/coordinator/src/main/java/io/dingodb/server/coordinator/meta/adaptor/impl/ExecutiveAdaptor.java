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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.store.MetaStore;
import io.dingodb.server.protocol.meta.Executive;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.OP_IDENTIFIER;

public class ExecutiveAdaptor extends BaseAdaptor<Executive> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.op, OP_IDENTIFIER.external);

    public ExecutiveAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Executive.class, this);
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected CommonId newId(Executive meta) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            0,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator
        implements BaseAdaptor.Creator<Executive, ExecutiveAdaptor> {
        @Override
        public ExecutiveAdaptor create(MetaStore metaStore) {
            return new ExecutiveAdaptor(metaStore);
        }
    }
}
