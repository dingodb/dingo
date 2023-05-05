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

package io.dingodb.calcite;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DingoRootSchema extends AbstractSchema {
    public static final String ROOT_SCHEMA_NAME = MetaService.ROOT_NAME;
    //public static final String DEFAULT_SCHEMA_NAME = ROOT_SCHEMA_NAME;
    public static final String DEFAULT_SCHEMA_NAME = MetaService.DINGO_NAME;

    private static final MetaService ROOT_META_SERVICE = MetaService.root();

    @Getter
    private final DingoParserContext context;
    @Getter
    private final List<String> names;

    DingoRootSchema(DingoParserContext context) {
        super();
        this.context = context;
        this.names = ImmutableList.of(ROOT_SCHEMA_NAME);
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return ROOT_META_SERVICE.getSubMetaServices().entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey().toUpperCase(),
                e -> new DingoSchema(context, names, ROOT_META_SERVICE.getSubMetaService(e.getKey())))
            );
    }

    public Schema getSubSchema(List<String> names) {
        // ignore 0 root schema
        Schema schema = this;
        for (int i = 1; i < names.size() - 1; i++) {
            schema = Parameters.nonNull(schema.getSubSchema(names.get(i)), "not found " + names);
        }
        return schema;
    }

}
