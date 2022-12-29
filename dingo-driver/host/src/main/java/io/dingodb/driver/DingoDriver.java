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

package io.dingodb.driver;

import io.dingodb.exec.impl.JobManagerImpl;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.util.Properties;

public class DingoDriver extends UnregisteredDriver {
    public static final DingoDriver INSTANCE = new DingoDriver();

    public static final String CONNECT_STRING_PREFIX = "jdbc:dingo:";

    static {
        INSTANCE.register();
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        return DingoFactory.class.getCanonicalName();
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DingoDriverVersion.INSTANCE;
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    public DingoMeta createMeta(AvaticaConnection connection) {
        return new DingoMeta((DingoConnection) connection, JobManagerImpl.INSTANCE);
    }

    public DingoConnection createConnection(String url, Properties info) {
        return ((DingoFactory) factory).newConnection(this, factory, url, info);
    }
}
