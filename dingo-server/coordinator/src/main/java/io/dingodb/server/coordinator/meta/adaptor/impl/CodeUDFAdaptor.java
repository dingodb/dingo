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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.meta.adaptor.Adaptor;
import io.dingodb.server.protocol.meta.CodeUDF;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.FUNCTION_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;

@AutoService(Adaptor.class)
public class CodeUDFAdaptor extends BaseAdaptor<CodeUDF> {

    public static final CommonId META_ID = CommonId.prefix(
        ID_TYPE.function, FUNCTION_IDENTIFIER.codeUDF, ROOT_DOMAIN + 1
    );
    private static final byte[] SEQ_PREFIX = META_ID.encode();

    public final Map<String, Integer> nameSeq = new ConcurrentHashMap<>();
    public final Map<String, Map<Integer, CodeUDF>> nameVersionUdf = new ConcurrentHashMap<>();

    @Override
    public Class<CodeUDF> adaptFor() {
        return CodeUDF.class;
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    public synchronized void reload() {
        super.reload();
        List<CommonId> deleteId = metaMap.values().stream()
            .filter(__ -> __.getState() == -1)
            .map(CodeUDF::getId)
            .collect(Collectors.toList());
        deleteId.forEach(metaMap::remove);

        Iterator<KeyValue> iterator = metaStore().keyValueScan(
            CommonId.prefix(ID_TYPE.function, FUNCTION_IDENTIFIER.codeUDF, ROOT_DOMAIN).encode()
        );
        while (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            nameSeq.put(new String(keyValue.getValue()), CommonId.decode(keyValue.getKey()).seq);
        }
        metaMap.values().forEach(udf -> {
            nameVersionUdf
                .computeIfAbsent(udf.getName(), __ -> new ConcurrentSkipListMap<>())
                .put(udf.getVersion(), udf);
        });
    }

    @Override
    protected synchronized CommonId newId(CodeUDF meta) {
        String name = meta.getName();
        Integer seq = nameSeq.get(name);
        if (seq == null) {
            seq = metaStore().generateSeq(SEQ_PREFIX);
            metaStore().upsertKeyValue(
                CommonId.prefix(META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, seq).encode(),
                name.getBytes()
            );
            nameSeq.put(name, seq);
        }
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            META_ID.domain,
            seq,
            metaStore().generateSeq(CommonId.prefix(META_ID.type, META_ID.identifier(), META_ID.domain, seq).encode())
        );
    }

    @Override
    protected void doSave(CodeUDF meta) {
        meta.setVersion(meta.getId().ver);
        super.doSave(meta);
        nameVersionUdf.computeIfAbsent(meta.getName(), __ -> new ConcurrentHashMap<>()).put(meta.getVersion(), meta);
    }

    public CodeUDF get(String name, int version) {
        return Optional.mapOrNull(nameVersionUdf.get(name), __ -> __.get(version));
    }

    public List<CodeUDF> get(String name) {
        return Optional.mapOrGet(nameVersionUdf.get(name), __ -> new ArrayList<>(__.values()), ArrayList::new);
    }

    public synchronized void unregister(String name, int version) {
        CodeUDF codeUDF = Optional.mapOrNull(nameVersionUdf.get(name), __ -> __.remove(version));
        if (codeUDF == null) {
            return;
        }
        delete(codeUDF.getId());
        nameVersionUdf.compute(name, (k, v) -> v == null || v.isEmpty() ? null : v);
    }

}
