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

import io.dingodb.common.CommonId;
import io.dingodb.server.coordinator.meta.store.MetaStore;
import io.dingodb.server.protocol.meta.CodeUDF;

import static io.dingodb.server.protocol.CommonIdConstant.FUNCTION_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;

public class UDFAdaptor extends BaseAdaptor<CodeUDF> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.function, FUNCTION_IDENTIFIER.codeUDF);

    public UDFAdaptor(MetaStore metaStore) {
        super(metaStore);
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected CommonId newId(CodeUDF meta) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            ROOT_DOMAIN,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode()),
            meta.getVersion()
        );
    }

}
