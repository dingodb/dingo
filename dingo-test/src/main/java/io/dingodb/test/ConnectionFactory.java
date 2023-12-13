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

package io.dingodb.test;

import io.dingodb.calcite.schema.DingoRootSchema;
import io.dingodb.common.CommonId;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.driver.DingoDriver;
import io.dingodb.exec.Services;
import io.dingodb.meta.local.LocalMetaService;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;

import static io.dingodb.common.CommonId.CommonType.DISTRIBUTION;

public final class ConnectionFactory {
    private ConnectionFactory() {
    }

    public static void initLocalEnvironment() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRole(DingoRole.JDBC);
        env.setInfo("user", "root");
        env.setInfo("password", "");

        // Configure for local test.
        DingoConfiguration.parse(
            Objects.requireNonNull(ConnectionFactory.class.getResource("/config.yaml")).getPath()
        );

        Services.initNetService();
        Services.NET.listenPort(DingoConfiguration.port());

        TreeMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> defaultDistribution = new TreeMap<>();
        byte[] startKey = ByteArrayUtils.EMPTY_BYTES;
        byte[] endKey = ByteArrayUtils.MAX;

        defaultDistribution.put(
            new ByteArrayUtils.ComparableByteArray(startKey),
            RangeDistribution.builder()
                .id(new CommonId(DISTRIBUTION, 1, 1))
                .startKey(startKey)
                .endKey(endKey)
                .build()
        );
        LocalMetaService metaService = LocalMetaService.ROOT;
        metaService.createSubMetaService(DingoRootSchema.DEFAULT_SCHEMA_NAME);
        metaService.setRangeDistributions(defaultDistribution);
        LocalMetaService.setLocation(new FakeLocation(0, DingoConfiguration.port()));
    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException, IOException {
        Class.forName("io.dingodb.driver.DingoDriver");
        Properties properties = new Properties();
        properties.load(ConnectionFactory.class.getResourceAsStream("/test.properties"));
        return DriverManager.getConnection(
            DingoDriver.CONNECT_STRING_PREFIX,
            properties
        );
    }

    public static void cleanUp() {
        LocalMetaService.clear();
    }
}
