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

package io.dingodb.server.coordinator.api;

import io.dingodb.server.coordinator.meta.adaptor.impl.CodeUDFAdaptor;
import io.dingodb.server.protocol.meta.CodeUDF;

import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;

public final class CodeUDFApi implements io.dingodb.server.api.CodeUDFApi {

    public static final CodeUDFApi INSTANCE = new CodeUDFApi();

    private CodeUDFApi() {
    }

    @Override
    public int register(String name, String code) {
        return getMetaAdaptor(CodeUDF.class).save(CodeUDF.builder().name(name).code(code).build()).ver;
    }

    @Override
    public void unregister(String name, int version) {
        ((CodeUDFAdaptor) getMetaAdaptor(CodeUDF.class)).unregister(name, version);
    }

    @Override
    public List<String> get(String name) {
        return ((CodeUDFAdaptor) getMetaAdaptor(CodeUDF.class)).get(name).stream()
            .map(CodeUDF::getCode)
            .collect(Collectors.toList());
    }

    @Override
    public String get(String name, int version) {
        return ((CodeUDFAdaptor) getMetaAdaptor(CodeUDF.class)).get(name, version).getCode();
    }
}
