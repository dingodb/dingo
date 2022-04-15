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
import io.dingodb.server.coordinator.TestBase;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.protocol.meta.Replica;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.SERVICE_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReplicaAdaptor extends TestBase {

    //@BeforeAll
    public static void beforeAll() throws Exception {
        TestBase.beforeAll();
    }

    //@AfterAll
    public static void afterAll() throws Exception {
        TestBase.afterAll();
    }

    //@Test
    public void testGetByExecutor() {
        ReplicaAdaptor adaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(1), encodeInt(1));
        CommonId executor1 = new CommonId(ID_TYPE.service, SERVICE_IDENTIFIER.executor, encodeInt(0), encodeInt(1));
        CommonId executor2 = new CommonId(ID_TYPE.service, SERVICE_IDENTIFIER.executor, encodeInt(0), encodeInt(2));
        CommonId executor3 = new CommonId(ID_TYPE.service, SERVICE_IDENTIFIER.executor, encodeInt(0), encodeInt(3));
        Replica replica1 = Replica.builder().host("node1").port(19191).part(partId).executor(executor1).build();
        Replica replica2 = Replica.builder().host("node2").port(19191).part(partId).executor(executor2).build();
        Replica replica3 = Replica.builder().host("node3").port(19191).part(partId).executor(executor3).build();
        adaptor.save(replica1);
        adaptor.save(replica2);
        adaptor.save(replica3);
        assertThat(adaptor.getByExecutor(executor1))
            .hasSize(1)
            .contains(replica1);
        assertThat(adaptor.getByExecutor(executor2))
            .hasSize(1)
            .contains(replica2);
        assertThat(adaptor.getByExecutor(executor3))
            .hasSize(1)
            .contains(replica3);
    }


}
