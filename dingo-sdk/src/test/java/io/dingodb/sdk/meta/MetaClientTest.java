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

package io.dingodb.sdk.meta;

import io.dingodb.common.CommonId;
import io.dingodb.sdk.client.MetaClient;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class MetaClientTest {

    public void testUdf() throws Exception {
        MetaClient meta = new MetaClient("127.0.0.1:19181,127.0.0.1:19182,127.0.0.1:19183");

        meta.init(null);

        CommonId id = meta.getTableId("PERSONS");

        String udf = meta.getUDF(id, "Test", 0);

        System.out.println("test" + udf);

        int version = meta.registerUDF(id, "Test", "testudfaaa");

        meta.unregisterUDF(id, "Test", 2);
        udf = meta.getUDF(id, "Test", 2);


        meta.unregisterUDF(id, "Test", 1);
        udf = meta.getUDF(id, "Test");
        System.out.println("test" + udf);

        udf = meta.getUDF(id, "Test", 1);
        System.out.println("test" + udf);


        int m10 = 10 * 1024 * 1024;
        byte[] m10b = new byte[m10];
        for (int i = 0; i < m10; i++) {
            m10b[i] = 1;
        }
        String m10s = new String(m10b, StandardCharsets.UTF_8);
        version = meta.registerUDF(id, "m10", m10s);
        String m10ss = meta.getUDF(id, "m10");
        System.out.println(m10ss.length());
        System.out.println(m10s.equals(m10ss));
    }
}
