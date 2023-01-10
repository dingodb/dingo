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

package io.dingodb.server.coordinator.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.server.coordinator.meta.adaptor.impl.CodeUDFAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ColumnAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutiveAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.PrivilegeAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.PrivilegeDictAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.SchemaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.SchemaPrivAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePrivAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.UserAdaptor;
import io.dingodb.server.protocol.meta.CodeUDF;
import io.dingodb.server.protocol.meta.Column;
import io.dingodb.server.protocol.meta.Executive;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.Index;
import io.dingodb.server.protocol.meta.MetaDefinitionBuilder;
import io.dingodb.server.protocol.meta.Privilege;
import io.dingodb.server.protocol.meta.PrivilegeDict;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.SchemaPriv;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePriv;
import io.dingodb.server.protocol.meta.User;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.meta.MetaService.DINGO_NAME;
import static io.dingodb.meta.MetaService.META_NAME;
import static io.dingodb.meta.MetaService.ROOT_NAME;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class Constant {

    public static final Map<Class<?>, Integer> ADAPTOR_SEQ_MAP = new HashMap<>();
    public static final Map<Class<?>, TableDefinition> ADAPTOR_DEFINITION_MAP = new HashMap<>();

    public static final int ROOT_SCHEMA_SEQ = 1;
    public static final int META_SCHEMA_SEQ = 2;
    public static final int DINGO_SCHEMA_SEQ = 3;

    public static final int SCHEMA_SEQ_START = 100;

    public static final CommonId ROOT_SCHEMA_ID = CommonId.prefix(
        ID_TYPE.table, TABLE_IDENTIFIER.schema, ROOT_DOMAIN, ROOT_SCHEMA_SEQ
    );
    public static final CommonId META_SCHEMA_ID = CommonId.prefix(
        ID_TYPE.table, TABLE_IDENTIFIER.schema, ROOT_DOMAIN, META_SCHEMA_SEQ
    );
    public static final CommonId DINGO_SCHEMA_ID = CommonId.prefix(
        ID_TYPE.table, TABLE_IDENTIFIER.schema, ROOT_DOMAIN, DINGO_SCHEMA_SEQ
    );

    public static final Schema ROOT_SCHEMA = Schema.builder().id(ROOT_SCHEMA_ID).name(ROOT_NAME).build();
    public static final Schema META_SCHEMA = Schema.builder().id(META_SCHEMA_ID).name(META_NAME).build();
    public static final Schema DINGO_SCHEMA = Schema.builder().id(DINGO_SCHEMA_ID).name(DINGO_NAME).build();

    static  {
        // set metaAdaptors [i] null when adaptor class remove
        List<Class<?>> metaAdaptors = Arrays.asList(
            ColumnAdaptor.class,
            ExecutiveAdaptor.class,
            ExecutorAdaptor.class,
            PrivilegeAdaptor.class,
            PrivilegeDictAdaptor.class,
            SchemaAdaptor.class,
            SchemaPrivAdaptor.class,
            TableAdaptor.class,
            TablePrivAdaptor.class,
            CodeUDFAdaptor.class,
            UserAdaptor.class
        );
        for (int i = 0; i < metaAdaptors.size(); i++) {
            if (metaAdaptors.get(i) != null) {
                ADAPTOR_SEQ_MAP.put(metaAdaptors.get(i), i);
            }
        }

        ADAPTOR_DEFINITION_MAP.put(
            CodeUDF.class,
            createDefinition(CodeUDF.class.getSimpleName() + "_META", toNames(CodeUDF.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Column.class,
            createDefinition(Column.class.getSimpleName() + "_META", toNames(Column.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Executive.class,
            createDefinition(Executive.class.getSimpleName() + "_META", toNames(Executive.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Executor.class,
            createDefinition(Executor.class.getSimpleName() + "_META", toNames(Executor.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Index.class,
            createDefinition(Index.class.getSimpleName() + "_META", toNames(Index.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Privilege.class,
            createDefinition(Privilege.class.getSimpleName() + "_META", toNames(Privilege.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            PrivilegeDict.class,
            createDefinition(PrivilegeDict.class.getSimpleName() + "_META", toNames(PrivilegeDict.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Replica.class,
            createDefinition(Replica.class.getSimpleName() + "_META", toNames(Replica.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Schema.class,
            createDefinition(Schema.class.getSimpleName() + "_META", toNames(Schema.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            SchemaPriv.class,
            createDefinition(SchemaPriv.class.getSimpleName() + "_META", toNames(SchemaPriv.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            Table.class,
            createDefinition(Table.class.getSimpleName() + "_META", toNames(Table.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            TablePart.class,
            createDefinition(TablePart.class.getSimpleName() + "_META", toNames(TablePart.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            TablePriv.class,
            createDefinition(TablePriv.class.getSimpleName() + "_META", toNames(TablePriv.Fields.values()))
        );
        ADAPTOR_DEFINITION_MAP.put(
            User.class,
            createDefinition(User.class.getSimpleName() + "_META", toNames(User.Fields.values()))
        );
    }

    private static List<String> toNames(Enum[] enums) {
        return Arrays.stream(enums)
            .map(Enum::name)
            .filter(name -> !name.equalsIgnoreCase("id"))
            .collect(Collectors.toList());
    }

    private static TableDefinition createDefinition(String name, List<String> names) {
        MetaDefinitionBuilder definitionBuilder = new MetaDefinitionBuilder(name);
        names.forEach(definitionBuilder::addColumn);
        return definitionBuilder.build();
    }

}
