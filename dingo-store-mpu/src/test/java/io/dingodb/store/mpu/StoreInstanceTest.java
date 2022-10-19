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

package io.dingodb.store.mpu;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

class StoreInstanceTest {

    private static final Path PATH = Paths.get(StoreInstance.class.getName());
    private static StoreInstance storeInstance;

    @BeforeAll
    public static void beforeAll() {
        FileUtils.createDirectories(PATH);
        CommonId id = CommonId.prefix((byte) 'T');
        storeInstance = new StoreInstance(id, PATH);
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .replicateLocations(Collections.singletonList(new Location("localhost", 0)))
            .replicateId(id)
            .replicates(Collections.singletonList(id))
            .build();
        storeInstance.assignPart(part);
    }

    @AfterAll
    public static void afterAll() {
        storeInstance.destroy();
        FileUtils.deleteIfExists(PATH);
    }

    @Test
    void testSetGet() {
        storeInstance.upsertKeyValue("test".getBytes(), "value".getBytes());
        System.out.println(new String(storeInstance.getValueByPrimaryKey("test".getBytes())));
    }

}
